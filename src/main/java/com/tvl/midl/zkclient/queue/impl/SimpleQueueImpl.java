package com.tvl.midl.zkclient.queue.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvl.midl.zkclient.ZkClientManager;
import com.tvl.midl.zkclient.exception.ZkQueueException;
import com.tvl.midl.zkclient.queue.IZkQueue;
import com.tvl.midl.zkclient.queue.NodeEntity;

/**
 * 分布式队列
 * 结构
 * --groupName
 *     --appname
 *         --queueRoot(持久节点)     
 *             --node1(持久顺序节点)
 *             --node2(持久顺序节点)
 *             --node3(持久顺序节点)
 *             
 * @description
 * @author st.z
 *
 */
public class SimpleQueueImpl implements IZkQueue {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientManager.class);

	private ZkClient zkClient;

	private volatile int size;
	
    private final ReentrantLock lock;
    private final Condition notEmpty;

	private IZkChildListener zkChildListener;

	// 本地缓存,提升读性能
	private ArrayBlockingQueue<NodeEntity> first100Cache; // index0 = child(0), index100=child(100)
	private ArrayBlockingQueue<NodeEntity> last100Cache; // index0 = child(size - 1), index100 = child(size-100)

	public String queueRoot;

	public SimpleQueueImpl(ZkClient zkClient) {
		this(zkClient, null);
	}

	public SimpleQueueImpl(ZkClient zkClient, String queueRoot) {
		this.zkClient = zkClient;
		if (StringUtils.isBlank(queueRoot)) {
			queueRoot = DEFAULT_QUEUE_ROOT;
		}
		this.queueRoot = queueRoot;
		this.first100Cache = new ArrayBlockingQueue<>(100);
		this.last100Cache = new ArrayBlockingQueue<>(100);
		this.lock = new ReentrantLock(false);
		this.notEmpty = lock.newCondition();
		this.zkChildListener = (parentPath, currentChilds) -> {
			fillCache(currentChilds);
			lock.lock();
			try {
				notEmpty.signal();
			}finally {
				lock.unlock();
			}
		};
		createRoot();
		registryListener();
		fillCache(null);
	}

	/**
	 * 递归创建根节点
	 */
	private void createRoot() {
		if (StringUtils.isBlank(this.queueRoot)) {
			this.queueRoot = DEFAULT_QUEUE_ROOT;
		}
		try {
			zkClient.createPersistent(this.queueRoot, true);
		} catch (ZkNodeExistsException e) {
			LOGGER.warn("---queueRoot:{}已存在！", this.queueRoot);
		}
	}

	private void registryListener() {
		this.zkClient.subscribeChildChanges(this.queueRoot, this.zkChildListener);
	}

	/**
	 * 从Zk上拉取数据填充本地缓存
	 * 注意：这里不能作删除操作,因为只是填充缓存,如果在未消费前删除ZK服务器上的数据，将有可能导致数据丢失
	 * 所以删除应在实际获取时执行。
	 * @param childs
	 */
	private void fillCache(List<String> childs) {
		// 拉取监听注册之前的数据
		if (childs == null) {
			childs = this.zkClient.getChildren(this.queueRoot);
		}
		if (childs == null || childs.isEmpty()) {

			// 对拉取到的子节点列表进行排序。
			Collections.sort(childs, (a, b) -> {
				return a.compareTo(b);
			});

			size = childs.size();
			int cacheIndex = size > 100 ? 100 : size;
			for (int i = 0; i < cacheIndex; i++) {
				String childNode = childs.get(i);
				String childNodePath = this.queueRoot.concat("/").concat(childNode);
				String content = zkClient.readData(childNodePath);
				if (content != null) { // 由于数据内容不允许为空，所以只有节点被删除的情况下这里才会为Null
					first100Cache.offer(new NodeEntity(childNodePath, content));
				}

				childNode = childs.get(size - 1 - i);
				childNodePath = this.queueRoot.concat("/").concat(childNode);
				content = zkClient.readData(childNodePath);
				if (content != null) {
					last100Cache.offer(new NodeEntity(childNodePath, content));
				}
			}
		}else {
			first100Cache.clear();
			last100Cache.clear();
			size = 0;
		}
	}

	@Override
	public String poll() {
		if (first100Cache.isEmpty()) { // 如果本地缓存消耗完毕,则继续下一次拉取
			fillCache(null);
		}
		NodeEntity entity = first100Cache.poll(); // 优先消耗本地缓存
		if (entity != null) {
			/*
			 * 分布式环境下，节点间缓存内容可能相同，所以可能导致多节点消耗同一元素,所以这里以删除成功为标记。
			 * 删除成功即意味着当前节点消耗成功，否则意味着其它节点已消耗该数据，则本节点继续下一轮读取。
			 */
			if (this.zkClient.delete(entity.getNodePath())) {
				return entity.getNodeData();
			}
			return poll();
		}
		return null;
	}

	@Override
	public String pollLast() {
		if (last100Cache.isEmpty()) { // 如果本地缓存消耗完毕,则继续下一次拉取
			fillCache(null);
		}
		NodeEntity entity = last100Cache.poll(); // 优先消耗本地缓存
		if (entity != null) {
			/*
			 * 分布式环境下，节点间缓存内容可能相同，所以可能导致多节点消耗同一元素,所以这里以删除成功为标记。
			 * 删除成功即意味着当前节点消耗成功，否则意味着其它节点已消耗该数据，则本节点继续下一轮读取。
			 */
			if (this.zkClient.delete(entity.getNodePath())) {
				return entity.getNodeData();
			}
			return pollLast();
		}
		return null;
	}
	
	@Override
	public String take() {
		lock.lock();
		try {
			while(isEmpty()) {
				notEmpty.await();
			}
			
			String content;
			while((content = poll()) == null) {
				notEmpty.await();
			}
			return content;
		}catch(Exception e) {
			LOGGER.error("----take exception:{}",e);
			throw new ZkQueueException(e.getMessage(),e);
		}finally {
			lock.unlock();
		}
	}

	@Override
	public boolean put(String content) {
		if (StringUtils.isBlank(content)) {
			throw new ZkQueueException("----content is null!");
		}
		final String queuePath = this.queueRoot.concat(PATH_PREFIX);
		zkClient.createPersistentSequential(queuePath, content);
		return true;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public int size() {
		return size;
	}

}
