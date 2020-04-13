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
			throw new ZkStartException("----zkConfig is null��");
		}
		if (StringUtils.isAnyBlank(zkConfig.getGroupName(), zkConfig.getAppName())) {
			throw new ZkStartException("----groupName��appName is null��");
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
				throw new ZkStartException("----zkClusterAddress��otherConfigAddress����ͬʱΪ�գ���ָ��zk��Ⱥ��ַ�����������������ȡ��ַ��");
			}
			zkConfig.setZkClusterAddress(""); // TODO:�ӵ���������������ȡ,ÿ10��ˢ��һ�Ρ�
		}
		if(StringUtils.isNotBlank(zkConfig.getChrootPath())) {
			zkConfig.setZkClusterAddress(zkConfig.getZkClusterAddress() + "/" + zkConfig.getChrootPath());
		}
		
		this.zkClient = new ZkClient(zkConfig.getZkClusterAddress(), zkConfig.getSessionTimeOut(), zkConfig.getConnectTimeOut(), new SerializableSerializer());
		this.idMarkerFactory = ZkSeqIdMakerFactory.getInstance();
		this.queueFactory = ZkQueueFactory.getInstance();
		
		//����Master-Slaveѡ��
		if(zkConfig.getEnableMaster() > 0) {
			startMasterElect();
		}
	}
	
	/**
	 * ========Masterѡ�����========
	 */
	private void startMasterElect() {
		final String masterRoot = zkConfig.getMasterRoot();
		final String nodesRoot = zkConfig.getNodesRoot();
		if(StringUtils.isAnyBlank(masterRoot,nodesRoot)) {
			throw new ZkStartException("----ѡ������ȱʧ,ԭ��:zkConfig.masterRoot or zkConfig.nodesRoot is null!");
		}
		IMasterElectCallBack callback;
		if((callback = zkConfig.getMasterElectCallback()) == null) {
			throw new ZkStartException("----ѡ������ȱʧ,ԭ��:zkConfig.masterElectCallback is null!");
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
	 * ========ID�������========
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
	 * ========�ֲ�ʽ����========
	 * @return
	 */
	public IZkQueue createZkSimpleQueue() {
		return  createZkSimpleQueue(null);
	}
	
	public IZkQueue createZkSimpleQueue(String queueRoot) {
		return  queueFactory.createSimpleZkQueue(this.zkClient,queueRoot);
	}
	
	/**
	 * ========�ֲ�ʽ�����========
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
	 * ��ƽ��:�Ƚ��ȳ�ԭ��
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
	 * �����������
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
	 * ���³�ʼ��zkClient
	 */
	public synchronized void restart() {
		if (this.zkClient != null) {
			this.zkClient.close();
		}
		init();
	}

}
