package com.tvl.midl.zkclient.config;

import java.util.Map;
import java.util.Properties;

public interface IZkConfigHandler {
	
	Map<String,Properties> loadConfig();
	
	void handleDataChange(String key,Properties prop);
}
