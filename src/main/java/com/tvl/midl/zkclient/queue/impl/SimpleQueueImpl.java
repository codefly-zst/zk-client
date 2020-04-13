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
 * �ֲ�ʽ����
 * �ṹ
 * --groupName
 *     --appname
 *         --queueRoot(�־ýڵ�)     
 *             --node1(�־�˳��ڵ�)
 *             --node2(�־�˳��ڵ�)
 *             --node3(�־�˳��ڵ�)
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

	// ���ػ���,����������
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
	 * �ݹ鴴�����ڵ�
	 */
	private void createRoot() {
		if (StringUtils.isBlank(this.queueRoot)) {
			this.queueRoot = DEFAULT_QUEUE_ROOT;
		}
		try {
			zkClient.createPersistent(this.queueRoot, true);
		} catch (ZkNodeExistsException e) {
			LOGGER.warn("---queueRoot:{}�Ѵ��ڣ�", this.queueRoot);
		}
	}

	private void registryListener() {
		this.zkClient.subscribeChildChanges(this.queueRoot, this.zkChildListener);
	}

	/**
	 * ��Zk����ȡ������䱾�ػ���
	 * ע�⣺���ﲻ����ɾ������,��Ϊֻ����仺��,�����δ����ǰɾ��ZK�������ϵ����ݣ����п��ܵ������ݶ�ʧ
	 * ����ɾ��Ӧ��ʵ�ʻ�ȡʱִ�С�
	 * @param childs
	 */
	private void fillCache(List<String> childs) {
		// ��ȡ����ע��֮ǰ������
		if (childs == null) {
			childs = this.zkClient.getChildren(this.queueRoot);
		}
		if (childs == null || childs.isEmpty()) {

			// ����ȡ�����ӽڵ��б��������
			Collections.sort(childs, (a, b) -> {
				return a.compareTo(b);
			});

			size = childs.size();
			int cacheIndex = size > 100 ? 100 : size;
			for (int i = 0; i < cacheIndex; i++) {
				String childNode = childs.get(i);
				String childNodePath = this.queueRoot.concat("/").concat(childNode);
				String content = zkClient.readData(childNodePath);
				if (content != null) { // �����������ݲ�����Ϊ�գ�����ֻ�нڵ㱻ɾ�������������Ż�ΪNull
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
		if (first100Cache.isEmpty()) { // ������ػ����������,�������һ����ȡ
			fillCache(null);
		}
		NodeEntity entity = first100Cache.poll(); // �������ı��ػ���
		if (entity != null) {
			/*
			 * �ֲ�ʽ�����£��ڵ�仺�����ݿ�����ͬ�����Կ��ܵ��¶�ڵ�����ͬһԪ��,����������ɾ���ɹ�Ϊ��ǡ�
			 * ɾ���ɹ�����ζ�ŵ�ǰ�ڵ����ĳɹ���������ζ�������ڵ������ĸ����ݣ��򱾽ڵ������һ�ֶ�ȡ��
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
		if (last100Cache.isEmpty()) { // ������ػ����������,�������һ����ȡ
			fillCache(null);
		}
		NodeEntity entity = last100Cache.poll(); // �������ı��ػ���
		if (entity != null) {
			/*
			 * �ֲ�ʽ�����£��ڵ�仺�����ݿ�����ͬ�����Կ��ܵ��¶�ڵ�����ͬһԪ��,����������ɾ���ɹ�Ϊ��ǡ�
			 * ɾ���ɹ�����ζ�ŵ�ǰ�ڵ����ĳɹ���������ζ�������ڵ������ĸ����ݣ��򱾽ڵ������һ�ֶ�ȡ��
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
