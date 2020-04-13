package com.tvl.midl.zkclient.queue.factory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;

import com.tvl.midl.zkclient.exception.ZkIdMakerException;
import com.tvl.midl.zkclient.queue.IZkQueue;
import com.tvl.midl.zkclient.queue.impl.SimpleQueueImpl;

public class ZkQueueFactory {

	private static final int MAX_COUNT = 10;

	private static ZkQueueFactory instance = new ZkQueueFactory();

	private final Map<String, IZkQueue> zkQueueMap = new ConcurrentHashMap<>();
	
	private Object lock = new Object();
	
	private ZkQueueFactory() {}

	public IZkQueue createSimpleZkQueue(ZkClient zkClient) {
		return createSimpleZkQueue(zkClient, null);
	}

	public IZkQueue createSimpleZkQueue(ZkClient zkClient, String queueRoot) {
		if (StringUtils.isBlank(queueRoot)) {
			queueRoot = IZkQueue.DEFAULT_QUEUE_ROOT;
		}
		IZkQueue zkQueue = zkQueueMap.get(queueRoot);
		if (zkQueue != null) {
			return zkQueue;
		}

		if (zkQueueMap.size() >= MAX_COUNT) {
			throw new ZkIdMakerException("----zkQueueMap.size��������:" + MAX_COUNT);
		}

		//֮������concurrmap������»�����������Ϊ����֮�⣬��Ӧ������ͬʱ����queuRoot��ͬ��simpleQueueImpl����
		//��Ȼ����ֻ��һ����Ч��������ںܶ��ݵ�ʱ��ʧЧ�����ö����俪���ܴ��漰������IO�����ȵȣ�����new��������б�Ҫ��ֹ��
		synchronized (lock) { 
			 zkQueue = zkQueueMap.get(queueRoot);
			 if(zkQueue == null) {
				 zkQueueMap.putIfAbsent(queueRoot, new SimpleQueueImpl(zkClient, queueRoot));
			 }
		}
		
		return zkQueueMap.get(queueRoot);
	}

	public void clear() {
		zkQueueMap.clear();
	}

	public static ZkQueueFactory getInstance() {
		return instance;
	}

}
