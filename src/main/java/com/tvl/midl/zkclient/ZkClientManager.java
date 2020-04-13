package com.tvl.midl.zkclient;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvl.midl.zkclient.exception.ZkStartException;
import com.tvl.midl.zkclient.id.factory.ZkSeqIdMakerFactory;
import com.tvl.midl.zkclient.lock.IDistributedLock;
import com.tvl.midl.zkclient.lock.impl.FairDistributedLock;
import com.tvl.midl.zkclient.lock.impl.SimpleDistributedLock;
import com.tvl.midl.zkclient.master.IMasterElectCallBack;
import com.tvl.midl.zkclient.master.IMasterElectRegistry;
import com.tvl.midl.zkclient.master.impl.DefaultMasterElectRegistry;
import com.tvl.midl.zkclient.master.server.MasterElectServer;
import com.tvl.midl.zkclient.queue.IZkQueue;
import com.tvl.midl.zkclient.queue.factory.ZkQueueFactory;

public class ZkClientManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientManager.class);
	
	private static final int CONNECT_TIME_OUT = 5000;

	private static final int SESSION_TIME_OUT = 5000;
	
	private MasterElectServer masterElectServer;
	
	private ZkSeqIdMakerFactory idMarkerFactory;
	
	private ZkQueueFactory queueFactory;

	private ZkConfig zkConfig;

	private ZkClient zkClient;

	public ZkClientManager(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}

	public void init() {
		if (zkConfig == null) {
			throw new ZkStartException("----zkConfig is null！");
		}
		if (StringUtils.isAnyBlank(zkConfig.getGroupName(), zkConfig.getAppName())) {
			throw new ZkStartException("----groupName、appName is null！");
		}
		if (StringUtils.isBlank(zkConfig.getPassword())) {
			zkConfig.setPassword(zkConfig.getGroupName() + zkConfig.getAppName());
		}
		if (zkConfig.getSessionTimeOut() < SESSION_TIME_OUT) {
			zkConfig.setSessionTimeOut(SESSION_TIME_OUT);
		}
		if (zkConfig.getConnectTimeOut() < CONNECT_TIME_OUT) {
			zkConfig.setConnectTimeOut(CONNECT_TIME_OUT);
		}
		if (StringUtils.isBlank(zkConfig.getZkClusterAddress())) {
			if (StringUtils.isBlank(zkConfig.getOtherConfigAddress())) {
				throw new ZkStartException("----zkClusterAddress、otherConfigAddress不能同时为空，请指定zk集群地址或第三方配置中心拉取地址！");
			}
			zkConfig.setZkClusterAddress(""); // TODO:从第三方配置中心拉取,每10秒刷新一次。
		}
		if(StringUtils.isNotBlank(zkConfig.getChrootPath())) {
			zkConfig.setZkClusterAddress(zkConfig.getZkClusterAddress() + "/" + zkConfig.getChrootPath());
		}
		
		this.zkClient = new ZkClient(zkConfig.getZkClusterAddress(), zkConfig.getSessionTimeOut(), zkConfig.getConnectTimeOut(), new SerializableSerializer());
		this.idMarkerFactory = ZkSeqIdMakerFactory.getInstance();
		this.queueFactory = ZkQueueFactory.getInstance();
		
		//启用Master-Slave选举
		if(zkConfig.getEnableMaster() > 0) {
			startMasterElect();
		}
	}
	
	/**
	 * ========Master选举相关========
	 */
	private void startMasterElect() {
		final String masterRoot = zkConfig.getMasterRoot();
		final String nodesRoot = zkConfig.getNodesRoot();
		if(StringUtils.isAnyBlank(masterRoot,nodesRoot)) {
			throw new ZkStartException("----选举配置缺失,原因:zkConfig.masterRoot or zkConfig.nodesRoot is null!");
		}
		IMasterElectCallBack callback;
		if((callback = zkConfig.getMasterElectCallback()) == null) {
			throw new ZkStartException("----选举配置缺失,原因:zkConfig.masterElectCallback is null!");
		}
		IMasterElectRegistry registry;
		if((registry = zkConfig.getMasterElectRegistry()) == null) {
			registry = new DefaultMasterElectRegistry();
			LOGGER.warn("----masterElectRegistry:{}",DefaultMasterElectRegistry.class.getName());
		}
		masterElectServer = new MasterElectServer(zkClient, masterRoot, nodesRoot, registry.genRegInfo(), callback);
		masterElectServer.start();
	}
	
	/**
	 * ========ID生成相关========
	 * @return
	 */
	public String generateId() {
		return idMarkerFactory.createZkSeqIdMarker(this.zkClient).generateId();
	}
	
	public int generateIntId() {
		return Integer.parseInt(generateId());
	}
	
	public String generateIdByVersion() {
		return idMarkerFactory.createZkSeqIdMarker(this.zkClient).generateIdByVersion();
	}
	
	public int generateIntIdByVersion() {
		return Integer.parseInt(generateIdByVersion());
	}
	
	/**
	 * ========分布式队列========
	 * @return
	 */
	public IZkQueue createZkSimpleQueue() {
		return  createZkSimpleQueue(null);
	}
	
	public IZkQueue createZkSimpleQueue(String queueRoot) {
		return  queueFactory.createSimpleZkQueue(this.zkClient,queueRoot);
	}
	
	/**
	 * ========分布式锁相关========
	 * @param fair
	 * @return
	 */
	public IDistributedLock getSimpleDistributedLock() {
		return new SimpleDistributedLock(this.zkClient);
	}
	
	public IDistributedLock getSimpleDistributedLock(String lockName) {
		return new SimpleDistributedLock(this.zkClient,lockName);
	}
	
	/**
	 * 公平锁:先进先出原则
	 * @param fair
	 * @param lockRoot
	 * @return
	 */
	public IDistributedLock getFairLock() {
		return new FairDistributedLock(this.zkClient);
	}
	
	public IDistributedLock getFairLock(String lockName) {
		return new FairDistributedLock(this.zkClient,lockName);
	}
	
	/**
	 * 配置中心相关
	 */
	
	
	public void destory() {
		if(this.idMarkerFactory != null) {
			idMarkerFactory.clear();
		}
		if(this.masterElectServer != null) {
			this.masterElectServer.stop();
		}
		if (this.zkClient != null) {
			this.zkClient.close();
		}
	}

	/**
	 * 重新初始化zkClient
	 */
	public synchronized void restart() {
		if (this.zkClient != null) {
			this.zkClient.close();
		}
		init();
	}

}
