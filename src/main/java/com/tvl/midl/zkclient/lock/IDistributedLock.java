package com.tvl.midl.zkclient.lock;

import java.util.concurrent.TimeUnit;

public interface IDistributedLock {

	/**
	 * 尝试获取锁，如果无法获取立即返回false。否则返回true
	 * 
	 * @return
	 * @throws Exception
	 */
//	public boolean tryLock() throws Exception;

	/**
	 * 尝试获取锁,直到超时。
	 * 为了避免countDown()操作发生在await()之前，导致死锁情况，这里不支持无限等待。
	 * 如果指定时间内获取成功则返回true，否则返回false。
	 * 
	 * @param time 超时时间
	 * @param unit 时间单位
	 * @return
	 * @throws Exception
	 */
	public boolean tryLock(long time, TimeUnit unit);

	/**
	 * 释放锁
	 * @throws Exception
	 */
	public void unlock();

}
