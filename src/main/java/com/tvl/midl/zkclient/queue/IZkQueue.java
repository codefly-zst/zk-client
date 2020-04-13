package com.tvl.midl.zkclient.queue;

public interface IZkQueue {
	
	static final String DEFAULT_QUEUE_ROOT = "/queue/simpleRoot";

	static final String PATH_PREFIX = "queue_";

	/**
	 * ��ͷ����ʼ����,���Ϊ���򷵻�Null
	 * 
	 * @return
	 */
	String poll();

	/**
	 * ��β����ʼ����,���Ϊ���򷵻�Null
	 * 
	 * @return
	 */
	String pollLast();

	/**
	 * ��ͷ����ʼ����,���Ϊ��������
	 * 
	 * @return
	 */
	String take();
	
	/**
	 * ��ӵ�β��
	 * 
	 * @param content
	 * @return
	 */
	boolean put(String content);

	/**
	 * �Ƿ�Ϊ��
	 * 
	 * @return
	 */
	boolean isEmpty();

	/**
	 * ��С
	 * 
	 * @return
	 */
	int size();

}
