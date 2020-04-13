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
 * �ֲ�ʽΨһID������ ����1������˳��ڵ�. ����2�����ð汾��
 * �ṹ:
 * --groupName
 *     --appname
 *         --idRoot(Ĭ�ϸ��ڵ�<�־ýڵ�>)
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
	 * �ݹ鴴�����ڵ�
	 */
	private void createRoot() {
		if (StringUtils.isBlank(this.idRoot)) {
			this.idRoot = DEFAULT_ID_ROOT;
		}
		zkClient.createPersistent(this.idRoot, true);
		hasRoot = true;
	}

	/**
	 * ====����1������˳��ڵ��ID���ɷ�ʽ
	 */
	public String generateId() {
		if (!hasRoot) {
			createRoot();
		}
		final String idSubPath = this.idRoot.concat(ID_PREFIX);
		final String path = zkClient.createPersistentSequential(idSubPath, null);

		singleThreadExecutor.execute(() -> { // �첽ʵʱ����ڵ�
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
	 * ====����2�����ڰ汾�ŵ�ID���ɷ�ʽ
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
