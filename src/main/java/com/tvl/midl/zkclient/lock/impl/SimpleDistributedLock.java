package com.tvl.midl.zkclient.lock.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvl.midl.zkclient.exception.ZkLockException;
import com.tvl.midl.zkclient.lock.IDistributedLock;

/**
 * 非公平锁实现
 * 结构
 * --groupName
 *     --appname
 *         --lockRoot(临时节点,竞争创建)
 *         
 * 注意：该对象为非线程安全，多个线程应该通过独立的该对象来获取锁, 一个锁对应一个tryLock，
 * 也就是说不管多少个new SimpleDistributedLock()对象，只其他们的lockRoot一样，就会产生锁竞争。
 * eg: 下面的使用方式将会产生安全问题。
 * public void t(){
 *  SimpleDistributedLock lock = new SimpleDistributedLock();
 *  new Thread(()->{lock.tryLock()...});
 *  new Thread(()->{lock.tryLock()...});
 * }
 * 应修改如下
 * public void t(){
 *  new Thread(()->{new SimpleDistributedLock().tryLock()...});  //与下面的SimpleDistributedLock对象的lockRoot一样即可
 *  new Thread(()->{new SimpleDistributedLock().tryLock()...});  //
 * }
 * 
 * @description
 * @author st.z
 *
 */
public class SimpleDistributedLock implements IDistributedLock {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDistributedLock.class);
	
	private static final String DEFAULT_LOCK_ROOT = "/simple_lock/";
	
	private static final String DEFAULT_LOCK_NAME = "lockRoot";

	private ZkClient zkClient;

	private String lockName;

	private IZkDataListener zkDataListener;

	private CountDownLatch countDownLatch;
	
	//用来实现锁重入
	private static final ThreadLocal<AtomicInteger> threadLock = new ThreadLocal<AtomicInteger>();

	public SimpleDistributedLock(ZkClient zkClient) {
		this(zkClient, DEFAULT_LOCK_ROOT);
	}

	public SimpleDistributedLock(ZkClient zkClient, String lockName) {
		if (StringUtils.isBlank(lockName)) {
			lockName = DEFAULT_LOCK_NAME;
		}
		this.zkClient = zkClient;
		this.lockName = lockName;
		this.zkDataListener = new IZkDataListener() {
			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				TimeUnit.MILLISECONDS.sleep(10);  //尽量避免该操作发生在countDownLatch.await()之前。
				if(countDownLatch != null) {
					countDownLatch.countDown();       //计数器等于0时，不会发生任何操作。
				}
			}
			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {
			}
		};
	}

	private boolean tryLock() {
		AtomicInteger count = threadLock.get();
		if(count !=  null) {
			threadLock.get().incrementAndGet();
			return true;
		}
		try {
			this.zkClient.createEphemeral(DEFAULT_LOCK_ROOT.concat(this.lockName), true);
			threadLock.set(new AtomicInteger(1));
			return true;
		}catch(Exception e) {
			LOGGER.error("----tryLock()失败,原因:{}",e);
			return false;
		}	
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) {
		if(time < 0) {
			throw new ZkLockException("----time is null!");
		}
		unit = (unit == null ? TimeUnit.MILLISECONDS : unit);
		long startMillis = System.currentTimeMillis();
		Long millisToWait = (unit != null) ? unit.toMillis(time) : null;
		while (true) {
			if(tryLock()) {
				return true;
			}
			//注册监听，监听失败<节点不存在>重新竞争
			try {
				this.zkClient.subscribeDataChanges(DEFAULT_LOCK_ROOT.concat(this.lockName), this.zkDataListener);
			}catch(ZkNoNodeException e1) {
				LOGGER.warn("----acquire异常:{}",e1);
				continue;
			}
			//剩余超时时间
			millisToWait -= (System.currentTimeMillis() - startMillis); 	
			startMillis = System.currentTimeMillis();
			try {
				this.countDownLatch = new CountDownLatch(1);
				if (!countDownLatch.await(millisToWait, TimeUnit.MILLISECONDS)) {	// 超时
					return false;
				}
			} catch (InterruptedException e) {
				throw new ZkLockException(e);
			}
		}
	}
	
	/**
	 * 如果调用release()后，当前线程持有计数器<0，则抛出异常
	 */
	@Override
	public void unlock() {
		AtomicInteger lockCount = threadLock.get();
		if(lockCount == null) {
			throw new ZkLockException("----release lock 失败！lockCount:null");
		}
		int count = lockCount.decrementAndGet();
		if (count > 0) {
			//不删除锁对应的节点
			return;
		}
		if (count < 0) {
			throw new ZkLockException("---release lock 失败！原因:锁计数器已经为负数");
		}
		try {
			this.zkClient.unsubscribeDataChanges(DEFAULT_LOCK_ROOT.concat(this.lockName), this.zkDataListener);
			this.zkClient.delete(DEFAULT_LOCK_ROOT.concat(this.lockName));  
		}finally {
			threadLock.remove(); 	//清除记数器
		}
	}

}
