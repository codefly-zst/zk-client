package com.tvl.midl.zkclient.master;

import java.util.Map;

/**
 * ��ҵ�񷽻ص�ʹ��
 * 
 * @description
 * @author st.z
 */
public interface IMasterElectCallBack {

	/**
	 * master�ڵ�仯ʱ�Ļص�������
	 * 
	 * @param isMaster       ��ǰ�ڵ��Ƿ�ΪMaster�ڵ�
	 * @param masterJsonInfo master�ڵ���Ϣ
	 * @param localJsonInfo  ��ǰ�ڵ���Ϣ
	 * @param nodeMap        ���нڵ��б�
	 * @return
	 */
	Object handleMasterDelete(boolean isMaster, String masterJsonInfo, String localJsonInfo, Map<String, String> nodeMap);

	/**
	 * �ڵ��б�仯ʱ�Ļص�������
	 * 
	 * @param isMaster       ��ǰ�ڵ��Ƿ�ΪMaster�ڵ�
	 * @param masterJsonInfo master�ڵ���Ϣ
	 * @param localJsonInfo  ��ǰ�ڵ���Ϣ
	 * @param nodeMap        ���нڵ��б�
	 * @return
	 */
	Object handleChildChange(boolean isMaster, String masterJsonInfo, String localJsonInfo, Map<String, String> nodeMap);
}
