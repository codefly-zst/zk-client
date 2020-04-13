package com.tvl.midl.zkclient.master;

/**
 * 选举注册器:用来生成注册信息
 *
 * @description
 * @author st.z
 */
public interface IMasterElectRegistry {

	/**
	 * 生成本机注册信息
	 * 
	 * @return
	 */
	String genRegInfo();

}
