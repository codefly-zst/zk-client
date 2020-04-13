package com.tvl.midl.zkclient.queue;

public interface IZkQueue {
	
	static final String DEFAULT_QUEUE_ROOT = "/queue/simpleRoot";

	static final String PATH_PREFIX = "queue_";

	/**
	 * 从头部开始消费,如果为空则返回Null
	 * 
	 * @return
	 */
	String poll();

	/**
	 * 从尾部开始消费,如果为空则返回Null
	 * 
	 * @return
	 */
	String pollLast();

	/**
	 * 从头部开始消费,如果为空则阻塞
	 * 
	 * @return
	 */
	String take();
	
	/**
	 * 添加到尾部
	 * 
	 * @param content
	 * @return
	 */
	boolean put(String content);

	/**
	 * 是否为空
	 * 
	 * @return
	 */
	boolean isEmpty();

	/**
	 * 大小
	 * 
	 * @return
	 */
	int size();

}
