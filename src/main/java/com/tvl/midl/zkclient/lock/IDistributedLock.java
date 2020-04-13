package com.tvl.midl.zkclient.lock;

import java.util.concurrent.TimeUnit;

public interface IDistributedLock {

	/**
	 * ���Ի�ȡ��������޷���ȡ��������false�����򷵻�true
	 * 
	 * @return
	 * @throws Exception
	 */
//	public boolean tryLock() throws Exception;

	/**
	 * ���Ի�ȡ��,ֱ����ʱ��
	 * Ϊ�˱���countDown()����������await()֮ǰ������������������ﲻ֧�����޵ȴ���
	 * ���ָ��ʱ���ڻ�ȡ�ɹ��򷵻�true�����򷵻�false��
	 * 
	 * @param time ��ʱʱ��
	 * @param unit ʱ�䵥λ
	 * @return
	 * @throws Exception
	 */
	public boolean tryLock(long time, TimeUnit unit);

	/**
	 * �ͷ���
	 * @throws Exception
	 */
	public void unlock();

}
