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
	
	// �����е��߳���Ϣ
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
				TimeUnit.MILLISECONDS.sleep(10);  //��������ò���������countDownLatch.await()֮ǰ��
				if(countDownLatch != null) {
					countDownLatch.countDown();       //����������0ʱ�����ᷢ���κβ�����
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
				// 0����ȡ���нڵ��б�,������
				final List<String> znodeList = zkClient.getChildren(this.lockPath);
				Collections.sort(znodeList, (a, b) -> {
					return a.compareTo(b);
				});
				// 1������Լ������Ľڵ�,�����б�ͷ������ɹ���ȡ
				int ourIndex = znodeList.indexOf(this.ourNodeName);
				if (ourIndex < 0) {
					throw new ZkLockException("----�ڵ�û���ҵ�: " + this.ourNodeName); // ��������ʱ�ڵ㣬���Կ��������������϶���ɾ����
				}
				if (ourIndex == 0) {
					return true;
				}
				// 2������ǰһ����ʱ�ڵ�<���ǰһ���ڵ㲻���ڣ���ݹ����ϲ���>
				this.countDownLatch = new CountDownLatch(1);
				String preNodeFullPath = null;
				for (int i = 0; i < ourIndex; i++) {
					final String preNodeName = znodeList.get(ourIndex - 1 - i);
					preNodeFullPath = this.lockPath.concat("/").concat(preNodeName);
					try {
						zkClient.subscribeDataChanges(preNodeFullPath, this.zkDataListener);
						break;
					} catch (ZkNoNodeException e) {
						LOGGER.warn("----��ʱ˳��ڵ�znode:{}�����ڣ�", preNodeFullPath);
						if (i == ourIndex - 1) {
							throw new ZkLockException(e);
						}
					}
				}
				// 3��await,��ʱ�򷵻�false
				if (System.currentTimeMillis() - startMillis - millisToWait <= 0) {
					return false;
				}
				if(!this.countDownLatch.await(millisToWait, TimeUnit.MILLISECONDS)) {
					return false;
				}
				
				// 4������֮��,�Ƴ���ǰһ��ʱ�ڵ�ļ����¼�
				// ע��:���ﲢ�������Ϸ���true��ԭ������ǰһ��ʱ�ڵ�����������綶����ɾ��������ǰ�ڵ㲢û��λ�ڵ�1��λ�ã�������Ҫ�����жϡ�
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
				// �����Լ�����ʱ�ڵ�
				this.ourNodeName = zkClient.createEphemeralSequential(this.lockPath.concat("/").concat(PATH_PREFIX),null);
				// �ȴ���ȡ��
				boolean lockOk = waitToLock(startMillis,millisToWait);
				if(lockOk) {
					LockData newLockData = new LockData(currentThread, ourNodeName);
					threadData.put(currentThread, newLockData);
				}
				return lockOk;
			} catch (Exception e) {
				if (retryCount++ < MAX_RETRY_COUNT) {
					// ʣ��ʱ��
					millisToWait -= (System.currentTimeMillis() - startMillis);
					if (millisToWait <= 0) {
						return false;
					}
					LOGGER.error("----tryLock failed! ԭ��:{},��ǰ���Դ���:{}", e.getMessage(), retryCount);
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
			throw new ZkLockException("----��ǰ�߳�δ������,�޷��ͷ�");
		}

		int newLockCount = lockData.lockCount.decrementAndGet();

		if (newLockCount > 0) {
			return;
		}
		if (newLockCount < 0) {
			throw new ZkLockException("----��ǰ�߳�:{" + currentThread.getName() + "}���������Ѿ�Ϊ����: ") ;
		}

		try {
			this.zkClient.delete(lockData.ourLockPath);
		} finally {
			threadData.remove(currentThread);
		}
	}

}
