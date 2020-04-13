package com.tvl.midl.zkclient.lock.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvl.midl.zkclient.exception.ZkLockException;
import com.tvl.midl.zkclient.lock.IDistributedLock;

public class FairDistributedLock implements IDistributedLock {

	private static final Logger LOGGER = LoggerFactory.getLogger(FairDistributedLock.class);

	private static final Integer MAX_RETRY_COUNT = 3;

	private static final String DEFAULT_LOCK_ROOT = "/fair_lock/";

	private static final String DEFAULT_LOCK_NAME = "lockRoot";

	private static final String PATH_PREFIX = "lock_";

	private ZkClient zkClient;

	private String lockPath;

	private CountDownLatch countDownLatch;
	
	private IZkDataListener zkDataListener;

	private String ourNodeName;
	
	// 进程中的线程信息
	private final ConcurrentMap<Thread, LockData> threadData = new ConcurrentHashMap<Thread, LockData>();

	private static class LockData {
		final String ourLockPath;
		final AtomicInteger lockCount = new AtomicInteger(1);

		private LockData(Thread owningThread, String ourLockPath) {
			this.ourLockPath = ourLockPath;
		}
	}

	public FairDistributedLock(ZkClient zkClient) {
		this(zkClient, DEFAULT_LOCK_ROOT);
	}

	public FairDistributedLock(ZkClient zkClient, String lockName) {
		if (StringUtils.isBlank(lockName)) {
			lockName = DEFAULT_LOCK_NAME;
		}
		this.zkClient = zkClient;
		this.lockPath = DEFAULT_LOCK_ROOT + lockName;
		this.zkDataListener = new IZkDataListener() {
			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				TimeUnit.MILLISECONDS.sleep(10);  //尽量避免该操作发生在countDownLatch.await()之前。
				if(countDownLatch != null) {
					countDownLatch.countDown();       //计数器等于0时，不会发生任何操作。
				}
			}
			@Override
			public void handleDataChange(String dataPath, Object data) {}
		};
		try {
			this.zkClient.createPersistent(this.lockPath, true);
		} catch (ZkNodeExistsException e) {
			LOGGER.warn(e.getMessage());
		}
	}

	private boolean waitToLock(Long startMillis,Long millisToWait) {
		try {
			while (true) {
				// 0、获取已有节点列表,并排序
				final List<String> znodeList = zkClient.getChildren(this.lockPath);
				Collections.sort(znodeList, (a, b) -> {
					return a.compareTo(b);
				});
				// 1、如果自己创建的节点,处于列表头部，则成功获取
				int ourIndex = znodeList.indexOf(this.ourNodeName);
				if (ourIndex < 0) {
					throw new ZkLockException("----节点没有找到: " + this.ourNodeName); // 由于是临时节点，所以可能由于网络闪断而被删除。
				}
				if (ourIndex == 0) {
					return true;
				}
				// 2、监听前一个临时节点<如果前一个节点不存在，则递归往上查找>
				this.countDownLatch = new CountDownLatch(1);
				String preNodeFullPath = null;
				for (int i = 0; i < ourIndex; i++) {
					final String preNodeName = znodeList.get(ourIndex - 1 - i);
					preNodeFullPath = this.lockPath.concat("/").concat(preNodeName);
					try {
						zkClient.subscribeDataChanges(preNodeFullPath, this.zkDataListener);
						break;
					} catch (ZkNoNodeException e) {
						LOGGER.warn("----临时顺序节点znode:{}不存在！", preNodeFullPath);
						if (i == ourIndex - 1) {
							throw new ZkLockException(e);
						}
					}
				}
				// 3、await,超时则返回false
				if (System.currentTimeMillis() - startMillis - millisToWait <= 0) {
					return false;
				}
				if(!this.countDownLatch.await(millisToWait, TimeUnit.MILLISECONDS)) {
					return false;
				}
				
				// 4、唤醒之后,移除对前一临时节点的监听事件
				// 注意:这里并不能马上返回true，原因在于前一临时节点可能由于网络抖动被删除，但当前节点并没有位于第1个位置，所以需要重新判断。
				this.zkClient.unsubscribeDataChanges(preNodeFullPath, this.zkDataListener);
			}
		} catch(Exception e){
			throw new ZkLockException(e);
		}
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) {
		Thread currentThread = Thread.currentThread();

		LockData lockData = threadData.get(currentThread);
		if (lockData != null) {
			lockData.lockCount.incrementAndGet();
			return true;
		}

		if (time < 0) {
			throw new ZkLockException("----time is null!");
		}
		unit = (unit == null ? TimeUnit.MILLISECONDS : unit);

		final long startMillis = System.currentTimeMillis();
		Long millisToWait = unit.toMillis(time);
		int retryCount = 0;

		while (true) {
			try {
				// 创建自己的临时节点
				this.ourNodeName = zkClient.createEphemeralSequential(this.lockPath.concat("/").concat(PATH_PREFIX),null);
				// 等待获取锁
				boolean lockOk = waitToLock(startMillis,millisToWait);
				if(lockOk) {
					LockData newLockData = new LockData(currentThread, ourNodeName);
					threadData.put(currentThread, newLockData);
				}
				return lockOk;
			} catch (Exception e) {
				if (retryCount++ < MAX_RETRY_COUNT) {
					// 剩余时限
					millisToWait -= (System.currentTimeMillis() - startMillis);
					if (millisToWait <= 0) {
						return false;
					}
					LOGGER.error("----tryLock failed! 原因:{},当前重试次数:{}", e.getMessage(), retryCount);
					continue;
				} 
				throw new ZkLockException(e);
			}
		}
	}

	@Override
	public void unlock() {
		Thread currentThread = Thread.currentThread();

		LockData lockData = threadData.get(currentThread);
		if (lockData == null) {
			throw new ZkLockException("----当前线程未持有锁,无法释放");
		}

		int newLockCount = lockData.lockCount.decrementAndGet();

		if (newLockCount > 0) {
			return;
		}
		if (newLockCount < 0) {
			throw new ZkLockException("----当前线程:{" + currentThread.getName() + "}锁计数器已经为负数: ") ;
		}

		try {
			this.zkClient.delete(lockData.ourLockPath);
		} finally {
			threadData.remove(currentThread);
		}
	}

}
