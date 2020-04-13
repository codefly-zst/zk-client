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
 * �ǹ�ƽ��ʵ��
 * �ṹ
 * --groupName
 *     --appname
 *         --lockRoot(��ʱ�ڵ�,��������)
 *         
 * ע�⣺�ö���Ϊ���̰߳�ȫ������߳�Ӧ��ͨ�������ĸö�������ȡ��, һ������Ӧһ��tryLock��
 * Ҳ����˵���ܶ��ٸ�new SimpleDistributedLock()����ֻ�����ǵ�lockRootһ�����ͻ������������
 * eg: �����ʹ�÷�ʽ���������ȫ���⡣
 * public void t(){
 *  SimpleDistributedLock lock = new SimpleDistributedLock();
 *  new Thread(()->{lock.tryLock()...});
 *  new Thread(()->{lock.tryLock()...});
 * }
 * Ӧ�޸�����
 * public void t(){
 *  new Thread(()->{new SimpleDistributedLock().tryLock()...});  //�������SimpleDistributedLock�����lockRootһ������
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
	
	//����ʵ��������
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
				TimeUnit.MILLISECONDS.sleep(10);  //��������ò���������countDownLatch.await()֮ǰ��
				if(countDownLatch != null) {
					countDownLatch.countDown();       //����������0ʱ�����ᷢ���κβ�����
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
			LOGGER.error("----tryLock()ʧ��,ԭ��:{}",e);
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
			//ע�����������ʧ��<�ڵ㲻����>���¾���
			try {
				this.zkClient.subscribeDataChanges(DEFAULT_LOCK_ROOT.concat(this.lockName), this.zkDataListener);
			}catch(ZkNoNodeException e1) {
				LOGGER.warn("----acquire�쳣:{}",e1);
				continue;
			}
			//ʣ�೬ʱʱ��
			millisToWait -= (System.currentTimeMillis() - startMillis); 	
			startMillis = System.currentTimeMillis();
			try {
				this.countDownLatch = new CountDownLatch(1);
				if (!countDownLatch.await(millisToWait, TimeUnit.MILLISECONDS)) {	// ��ʱ
					return false;
				}
			} catch (InterruptedException e) {
				throw new ZkLockException(e);
			}
		}
	}
	
	/**
	 * �������release()�󣬵�ǰ�̳߳��м�����<0�����׳��쳣
	 */
	@Override
	public void unlock() {
		AtomicInteger lockCount = threadLock.get();
		if(lockCount == null) {
			throw new ZkLockException("----release lock ʧ�ܣ�lockCount:null");
		}
		int count = lockCount.decrementAndGet();
		if (count > 0) {
			//��ɾ������Ӧ�Ľڵ�
			return;
		}
		if (count < 0) {
			throw new ZkLockException("---release lock ʧ�ܣ�ԭ��:���������Ѿ�Ϊ����");
		}
		try {
			this.zkClient.unsubscribeDataChanges(DEFAULT_LOCK_ROOT.concat(this.lockName), this.zkDataListener);
			this.zkClient.delete(DEFAULT_LOCK_ROOT.concat(this.lockName));  
		}finally {
			threadLock.remove(); 	//���������
		}
	}

}
