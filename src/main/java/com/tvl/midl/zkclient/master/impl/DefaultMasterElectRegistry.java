package com.tvl.midl.zkclient.master.impl;

import com.tvl.midl.zkclient.master.IMasterElectRegistry;
import com.tvl.midl.zkclient.util.IpUtil;

/**
 * ѡ��Ĭ��ע����
 * 
 * @description
 * @author st.z
 *
 */
public class DefaultMasterElectRegistry implements IMasterElectRegistry {

	@Override
	public String genRegInfo() {
		return IpUtil.getIp();
	}

}
