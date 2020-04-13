package com.tvl.midl.zkclient.master.server;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tvl.midl.zkclient.master.IMasterElectCallBack;
import com.tvl.midl.zkclient.util.IpUtil;

/**
 * Masterѡ��
 * �ṹ:
 * --groupName
 *     --appname
 *         --masterRoot(��ʱ�ڵ�)
 *         --nodeParentPath(�־ýڵ�)
 *             --node1(��ʱ�ڵ�)
 *             --node2(��ʱ�ڵ�)
 *             --node3(��ʱ�ڵ�)
 * @description
 * @author st.z
 */
public class MasterElectServer{

	private static final Logger LOGGER = LoggerFactory.getLogger(MasterElectServer.class);

	// master��ʱ�ڵ�·��
	private String masterRoot;

	// node�б�ĸ��ڵ�·��
	private String nodesRoot;

	private String localJsonInfo;

	private String masterJsonInfo;

	private Map<String, String> nodeMap = new ConcurrentHashMap<String, String>();
	
	private IZkDataListener zkDataListener;
	
	private IZkChildListener zkChildListener;
	
	private IMasterElectCallBack callback;
	
	private volatile boolean isMaster; 
	
	private ZkClient zkClient;

	public MasterElectServer(ZkClient zkClient, String masterRoot, String nodesRoot, String localJsonInfo, IMasterElectCallBack callback) {
		this.zkClient = zkClient;
		this.masterRoot = masterRoot;
		this.nodesRoot = nodesRoot;
		this.callback = callback;
		this.zkDataListener = new IZkDataListener() {
			@Override
			public void handleDataChange(String dataPath, Object data) throws Exception {}

			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				// ��ֹ���������(���綶��),ĳ��ʱ��δ���ͬʱ����Master�������ͨ�����жϼ��ӳ�ʱ�䱣֤����ԭ������Master����ע�ᡣ
				if (masterJsonInfo == null || !masterJsonInfo.equals(localJsonInfo)) {
					TimeUnit.SECONDS.sleep(3);
				}
				takeMaster();
				if(MasterElectServer.this.callback != null) {
					MasterElectServer.this.callback.handleMasterDelete(isMaster,masterJsonInfo,localJsonInfo,nodeMap);
				}
			}
		};
		
		this.zkChildListener = (parentPath, childs) -> {
			this.nodeMap.clear();
			if (childs != null && !childs.isEmpty()) {
				for (String ch : childs) {
					String nodePath = nodesRoot.concat("/").concat(ch);
					nodeMap.put(nodePath, zkClient.readData(nodePath));
				}
			}
			if(MasterElectServer.this.callback != null) {
				MasterElectServer.this.callback.handleChildChange(isMaster, masterRoot, nodesRoot, nodeMap);
			}
		};
		// ע��Master�ڵ�����¼�(ÿ��ѡ�ٶ�������ע���¼�) 
		LOGGER.info("----registryMaterListenEvent");
		registryMaterListenEvent();
		
		// ע��ڵ��б�仯�����¼�
		LOGGER.info("----registryServerListenEvent");
		registryServerListenEvent();
		
		this.start();
	}

	public synchronized void start() {
		takeMaster();
	}

	private void takeMaster() {
		// 0��ѡ��
		LOGGER.info("----Masterѡ�ٿ�ʼ...");
		try {
			zkClient.createEphemeral(masterRoot, localJsonInfo);
			masterJsonInfo = localJsonInfo;
			isMaster = true;
		} catch (ZkNodeExistsException e) {
			isMaster = false;
			String masterData = zkClient.readData(masterRoot, true);
			if (masterData == null) {
				LOGGER.warn("----Masterѡ�ٻ�ȡmasterPath�ڵ������쳣,��ʼ��һ��ѡ��");
				takeMaster();
			} else {
				masterJsonInfo = masterData;
			}
		}
		LOGGER.info("----Masterѡ�ٽ���:Master��Ϣ:{}", masterJsonInfo);

		// 1����ʼ���ڵ��б�(ÿ��ѡ�ٶ�����սڵ��б�)
		initNodeList();
		LOGGER.info("----initNodeList:{}",nodeMap);

		// 2������ǰnode��Ϣ��Ϊ�ӽڵ㣬ע�ᵽnodesRoot��
		LOGGER.info("----registryCurrNode");
		registryCurrNode();
	}

	private void initNodeList() {
		this.nodeMap.clear();
		try {
			List<String> childs = this.zkClient.getChildren(nodesRoot);
			if (childs != null && !childs.isEmpty()) {
				for (String ch : childs) {
					String nodePath = this.nodesRoot.concat("/").concat(ch);
					this.nodeMap.put(nodePath, zkClient.readData(nodePath));
				}
			}
		} catch (ZkNoNodeException e) {
			try {
				zkClient.createPersistent(nodesRoot);
			} catch (ZkNodeExistsException e1) {
				initNodeList();
			}
		}
	}

	/**
	 * ��ֹ�ظ�ע��:���Ƴ�
	 */
	private void registryMaterListenEvent() {
//		zkClient.unsubscribeDataChanges(masterRoot,  this.zkDataListener);
		//(���ڵײ�ʹ��set<Lister>,���Լ�ʹ�����Ƴ�Ҳ����ע�����¼�����,��������unsubscribeDataChangesҲ����ʡ��)
		zkClient.subscribeDataChanges(masterRoot, this.zkDataListener);
	}

	/**
	 * ��ֹ�ظ�ע��:���Ƴ�
	 */
	private void registryServerListenEvent() {
//		zkClient.unsubscribeChildChanges(nodesRoot, this.zkChildListener);
		zkClient.subscribeChildChanges(nodesRoot, this.zkChildListener);
	}
	
	private void registryCurrNode() {
		try {
			this.zkClient.createEphemeral(nodesRoot.concat("/").concat(IpUtil.getIp()), localJsonInfo);
		}catch (ZkNodeExistsException e) {
			LOGGER.error("----node:{}�Ѵ���!",IpUtil.getIp());
		}
	}
	
	public synchronized void stop() {
		try {
			zkClient.unsubscribeDataChanges(masterRoot,  this.zkDataListener);
			zkClient.unsubscribeChildChanges(nodesRoot, this.zkChildListener);
		}finally {
			if(this.nodeMap != null) {
				this.nodeMap.clear();
			}
		}
	}

}
