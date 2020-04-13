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
 * Master选举
 * 结构:
 * --groupName
 *     --appname
 *         --masterRoot(临时节点)
 *         --nodeParentPath(持久节点)
 *             --node1(临时节点)
 *             --node2(临时节点)
 *             --node3(临时节点)
 * @description
 * @author st.z
 */
public class MasterElectServer{

	private static final Logger LOGGER = LoggerFactory.getLogger(MasterElectServer.class);

	// master临时节点路径
	private String masterRoot;

	// node列表的父节点路径
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
				// 防止假死情况下(网络抖动),某个时间段存在同时两个Master的情况。通过该判断及延迟时间保证，让原假死的Master优先注册。
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
		// 注册Master节点监听事件(每次选举都将重新注册事件) 
		LOGGER.info("----registryMaterListenEvent");
		registryMaterListenEvent();
		
		// 注册节点列表变化监听事件
		LOGGER.info("----registryServerListenEvent");
		registryServerListenEvent();
		
		this.start();
	}

	public synchronized void start() {
		takeMaster();
	}

	private void takeMaster() {
		// 0、选举
		LOGGER.info("----Master选举开始...");
		try {
			zkClient.createEphemeral(masterRoot, localJsonInfo);
			masterJsonInfo = localJsonInfo;
			isMaster = true;
		} catch (ZkNodeExistsException e) {
			isMaster = false;
			String masterData = zkClient.readData(masterRoot, true);
			if (masterData == null) {
				LOGGER.warn("----Master选举获取masterPath节点数据异常,开始下一次选举");
				takeMaster();
			} else {
				masterJsonInfo = masterData;
			}
		}
		LOGGER.info("----Master选举结束:Master信息:{}", masterJsonInfo);

		// 1、初始化节点列表(每次选举都将清空节点列表)
		initNodeList();
		LOGGER.info("----initNodeList:{}",nodeMap);

		// 2、将当前node信息作为子节点，注册到nodesRoot下
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
	 * 防止重复注册:先移除
	 */
	private void registryMaterListenEvent() {
//		zkClient.unsubscribeDataChanges(masterRoot,  this.zkDataListener);
		//(由于底层使用set<Lister>,所以即使不先移除也不会注册多个事件监听,因此上面的unsubscribeDataChanges也可以省略)
		zkClient.subscribeDataChanges(masterRoot, this.zkDataListener);
	}

	/**
	 * 防止重复注册:先移除
	 */
	private void registryServerListenEvent() {
//		zkClient.unsubscribeChildChanges(nodesRoot, this.zkChildListener);
		zkClient.subscribeChildChanges(nodesRoot, this.zkChildListener);
	}
	
	private void registryCurrNode() {
		try {
			this.zkClient.createEphemeral(nodesRoot.concat("/").concat(IpUtil.getIp()), localJsonInfo);
		}catch (ZkNodeExistsException e) {
			LOGGER.error("----node:{}已存在!",IpUtil.getIp());
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
