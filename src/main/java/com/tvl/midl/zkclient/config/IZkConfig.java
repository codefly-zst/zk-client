package com.tvl.midl.zkclient.config;

import java.util.Properties;

public interface IZkConfig {
	
	void init();

	void uploadConfig(String key, Properties properties);
	
	Properties readConfig(String key);

	Object readConfig(String key, String prop);
	
	void destroy();
}
