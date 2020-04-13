package com.tvl.midl.zkclient.id;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式唯一ID生成器 方案1：利用顺序节点. 方案2：利用版本号
 * 结构:
 * --groupName
 *     --appname
 *         --idRoot(默认根节点<持久节点>)
 * 
 * @description
 * @author st.z
 */
public class ZkSeqIdMaker {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZkSeqIdMaker.class);

	public static final String DEFAULT_ID_ROOT = "/idroot";

	private static final String ID_PREFIX = "seq_";
	
	protected String idRoot;
	
	private ZkClient zkClient;

	private volatile boolean hasRoot;
	
	private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setName(ZkSeqIdMaker.class.getSimpleName() + "-id-clean-thread");
			t.setDaemon(true);
			return t;
		}
	});

	public ZkSeqIdMaker(ZkClient zkClient) {
		this(zkClient, null);
	}

	public ZkSeqIdMaker(ZkClient zkClient, String root) {
		if(StringUtils.isBlank(root)) {
			root = DEFAULT_ID_ROOT;
		}
		this.idRoot = root;
	}

	/**
	 * 递归创建根节点
	 */
	private void createRoot() {
		if (StringUtils.isBlank(this.idRoot)) {
			this.idRoot = DEFAULT_ID_ROOT;
		}
		zkClient.createPersistent(this.idRoot, true);
		hasRoot = true;
	}

	/**
	 * ====方案1：基于顺序节点的ID生成方式
	 */
	public String generateId() {
		if (!hasRoot) {
			createRoot();
		}
		final String idSubPath = this.idRoot.concat(ID_PREFIX);
		final String path = zkClient.createPersistentSequential(idSubPath, null);

		singleThreadExecutor.execute(() -> { // 异步实时清理节点
			boolean result = this.zkClient.delete(path);
			if (!result) {
				LOGGER.warn("{}----Thread:{},delete:{},failed!", Thread.currentThread().getName(), path);
			}
		});
		return substringId(path);
	}

	private String substringId(String path) {
		if (StringUtils.isBlank(path)) {
			return null;
		}
		int i = path.lastIndexOf(ID_PREFIX);
		return path.substring(i + ID_PREFIX.length());
	}

	/**
	 * ====方案2：基于版本号的ID生成方式
	 * 
	 * @return
	 */
	public String generateIdByVersion() {
		if (!hasRoot) {
			createRoot();
		}
		Stat stat = zkClient.writeDataReturnStat(this.idRoot, "obj", -1);

		return String.valueOf(stat.getVersion());
	}
}
