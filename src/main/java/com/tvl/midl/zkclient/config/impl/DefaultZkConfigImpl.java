package com.tvl.midl.zkclient.config.impl;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.tvl.midl.zkclient.config.IZkConfig;
import com.tvl.midl.zkclient.config.IZkConfigHandler;

/**
 * ��������:���ǵ������������п��ܳ���1M,�Լ�����Key�϶�������Ϊ�˱��ⵥ�ڵ��������ݹ��࣬�Լ��ڵ�������������⣬
 * �������Ｔû�в��ý���������д�뵽һ���ڵ㣬Ҳû�в���ÿ��Key��Ӧһ���ڵ㡣
 * ������Ӧ�÷��������ڵ��������Լ�ÿ���ڵ�����ݡ�
 */
public class DefaultZkConfigImpl implements IZkConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultZkConfigImpl.class);

	private static final String DEFAULT_CONFIG_ROOT = "/config_root/";
	
	private ZkClient zkClient;
	
	private IZkConfigHandler zkConfigHandler;
	
	private IZkChildListener zkChildListener;
	
	private Map<String,Properties> config =  new ConcurrentHashMap<>();
	
	private Map<String,IZkDataListener> listenerMap = new ConcurrentHashMap<>();

	public DefaultZkConfigImpl(ZkClient zkClient,List<String> listRoot,IZkConfigHandler handler) {
		this.zkClient = zkClient;
		this.zkConfigHandler = handler;
		this.config = handler.loadConfig();
		this.zkChildListener = (parentPath,currentChilds) ->{
			if(currentChilds != null && !currentChilds.isEmpty()) {
				for(String ch : currentChilds) {
					String str = this.zkClient.readData(DEFAULT_CONFIG_ROOT.concat(ch));
					config.put(ch, JSONObject.parseObject(str, Properties.class));
				}
			}
		};
		this.init();
	}
	
	public void init() {
		//0���������ڵ㡢ע��ڵ��б����
		try {
			this.zkClient.createPersistent(DEFAULT_CONFIG_ROOT,true);
			this.zkClient.subscribeChildChanges(DEFAULT_CONFIG_ROOT, this.zkChildListener);
		} catch (ZkNodeExistsException e) {
			LOGGER.warn("----ZkNodeExistsException:{},ignore!", e.getMessage());
		}
		//1���������ýڵ㡢��ע���������������ýڵ��Ѵ��ڣ���ֱ�Ӹ������ݣ�
		if(this.config != null) {
			for(Map.Entry<String, Properties> entry : this.config.entrySet()) {
				try {
					this.zkClient.createPersistent(DEFAULT_CONFIG_ROOT.concat(entry.getKey()),JSONObject.toJSONString(entry.getValue()));
				}catch (ZkNodeExistsException e) {
					//����ڵ��Ѵ��ڣ�����½ڵ����ݣ���ҪӦ�Է��������������
					this.uploadConfig(DEFAULT_CONFIG_ROOT.concat(entry.getKey()),entry.getValue());
					//...
				}
				IZkDataListener zkDataListener = new IZkDataListener() {
					@Override
					public void handleDataDeleted(String dataPath) throws Exception {}
					
					@Override
					public void handleDataChange(String dataPath, Object data) throws Exception {
						//���±�������
						config.put(dataPath, JSONObject.parseObject(data.toString(), Properties.class));
						zkConfigHandler.handleDataChange(dataPath,JSONObject.parseObject(data.toString(), Properties.class) );
					}
				};
				this.zkClient.subscribeDataChanges(DEFAULT_CONFIG_ROOT.concat(entry.getKey()), zkDataListener);
				this.listenerMap.put(entry.getKey(), zkDataListener);
			}
		}
	}

	@Override
	public void uploadConfig(String key,Properties properties) {
		boolean exists = this.zkClient.exists(DEFAULT_CONFIG_ROOT.concat(key));
		if(exists) {
			this.zkClient.writeData(DEFAULT_CONFIG_ROOT.concat(key), JSONObject.toJSONString(properties));
		}else {
			try {
				this.zkClient.createPersistent(DEFAULT_CONFIG_ROOT.concat(key), JSONObject.toJSONString(properties));
			}catch(Exception e) {
				//...
			}
			IZkDataListener zkDataListener = new IZkDataListener() {
				@Override
				public void handleDataDeleted(String dataPath) throws Exception {}
				
				@Override
				public void handleDataChange(String dataPath, Object data) throws Exception {
					zkConfigHandler.handleDataChange(dataPath,JSONObject.parseObject(data.toString(), Properties.class) );
				}
			};
			this.listenerMap.put(key, zkDataListener);
		}
		this.config.put(key, properties); //���±��ػ���
	}

	@Override
	public Object readConfig(String key, String prop) {
		Properties props = readConfig(key);
		if(props != null) {
			return props.get(key);
		}
		return null;
	}
	
	@Override
	public Properties readConfig(String key) {
		return this.config.get(key);
//		boolean exists = this.zkClient.exists(DEFAULT_CONFIG_ROOT.concat(key));
//		if(exists) {
//			String str = this.zkClient.readData(DEFAULT_CONFIG_ROOT.concat(key));
//			return JSONObject.parseObject(str, Properties.class);
//		}
//		return null;
	}

	@Override
	public void destroy() {
		if(this.config != null) {
			this.config.clear();
		}
		if(this.listenerMap != null) {
			for(Map.Entry<String, IZkDataListener> entry : this.listenerMap.entrySet()) {
				this.zkClient.unsubscribeDataChanges(DEFAULT_CONFIG_ROOT.concat(entry.getKey()), entry.getValue());
			}
			this.listenerMap.clear();
		}
		this.zkClient.unsubscribeChildChanges(DEFAULT_CONFIG_ROOT,this.zkChildListener);
	}
	
}
