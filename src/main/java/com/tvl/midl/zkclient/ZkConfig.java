package com.tvl.midl.zkclient;

import com.tvl.midl.zkclient.master.IMasterElectCallBack;
import com.tvl.midl.zkclient.master.IMasterElectRegistry;

/**
 * Zk≈‰÷√¿‡
 * 
 * @description
 * @author st.z
 *
 */
public class ZkConfig {

	private String groupName;

	private String appName;

	private String chrootPath;

	private String password;

	private String zkClusterAddress;

	private String otherConfigAddress;

	private int enableMaster;

	private String masterRoot = "/elect/masterRoot";

	private String nodesRoot = "/elect/nodesRoot";

	private int connectTimeOut;

	private int sessionTimeOut;

	private IMasterElectCallBack masterElectCallback;

	private IMasterElectRegistry masterElectRegistry;

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getChrootPath() {
		return chrootPath;
	}

	public void setChrootPath(String chrootPath) {
		this.chrootPath = chrootPath;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getZkClusterAddress() {
		return zkClusterAddress;
	}

	public void setZkClusterAddress(String zkClusterAddress) {
		this.zkClusterAddress = zkClusterAddress;
	}

	public String getOtherConfigAddress() {
		return otherConfigAddress;
	}

	public void setOtherConfigAddress(String otherConfigAddress) {
		this.otherConfigAddress = otherConfigAddress;
	}

	public int getEnableMaster() {
		return enableMaster;
	}

	public void setEnableMaster(int enableMaster) {
		this.enableMaster = enableMaster;
	}

	public String getMasterRoot() {
		return masterRoot;
	}

	public void setMasterRoot(String masterRoot) {
		this.masterRoot = masterRoot;
	}

	public String getNodesRoot() {
		return nodesRoot;
	}

	public void setNodesRoot(String nodesRoot) {
		this.nodesRoot = nodesRoot;
	}

	public int getConnectTimeOut() {
		return connectTimeOut;
	}

	public void setConnectTimeOut(int connectTimeOut) {
		this.connectTimeOut = connectTimeOut;
	}

	public int getSessionTimeOut() {
		return sessionTimeOut;
	}

	public void setSessionTimeOut(int sessionTimeOut) {
		this.sessionTimeOut = sessionTimeOut;
	}

	public IMasterElectCallBack getMasterElectCallback() {
		return masterElectCallback;
	}

	public void setMasterElectCallback(IMasterElectCallBack masterElectCallback) {
		this.masterElectCallback = masterElectCallback;
	}

	public IMasterElectRegistry getMasterElectRegistry() {
		return masterElectRegistry;
	}

	public void setMasterElectRegistry(IMasterElectRegistry masterElectRegistry) {
		this.masterElectRegistry = masterElectRegistry;
	}

}
