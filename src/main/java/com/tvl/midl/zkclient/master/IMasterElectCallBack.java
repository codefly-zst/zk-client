package com.tvl.midl.zkclient.master;

import java.util.Map;

/**
 * 供业务方回调使用
 * 
 * @description
 * @author st.z
 */
public interface IMasterElectCallBack {

	/**
	 * master节点变化时的回调处理器
	 * 
	 * @param isMaster       当前节点是否为Master节点
	 * @param masterJsonInfo master节点信息
	 * @param localJsonInfo  当前节点信息
	 * @param nodeMap        所有节点列表
	 * @return
	 */
	Object handleMasterDelete(boolean isMaster, String masterJsonInfo, String localJsonInfo, Map<String, String> nodeMap);

	/**
	 * 节点列表变化时的回调处理器
	 * 
	 * @param isMaster       当前节点是否为Master节点
	 * @param masterJsonInfo master节点信息
	 * @param localJsonInfo  当前节点信息
	 * @param nodeMap        所有节点列表
	 * @return
	 */
	Object handleChildChange(boolean isMaster, String masterJsonInfo, String localJsonInfo, Map<String, String> nodeMap);
}
