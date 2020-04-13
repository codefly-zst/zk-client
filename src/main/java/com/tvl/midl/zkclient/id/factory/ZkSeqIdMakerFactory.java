package com.tvl.midl.zkclient.id.factory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;

import com.tvl.midl.zkclient.exception.ZkIdMakerException;
import com.tvl.midl.zkclient.id.ZkSeqIdMaker;

/**
 * ID生成器工厂类对象
 * 
 * @description
 * @author st.z
 *
 */
public class ZkSeqIdMakerFactory {

	private static final int MAX_COUNT = 10;

	private static ZkSeqIdMakerFactory instance = new ZkSeqIdMakerFactory();
	
	private final Map<String, ZkSeqIdMaker> idMarkerMap = new ConcurrentHashMap<>();
	
	private ZkSeqIdMakerFactory() {}

	public ZkSeqIdMaker createZkSeqIdMarker(ZkClient zkClient) {
		return createZkSeqIdMarker(zkClient, null);
	}

	public ZkSeqIdMaker createZkSeqIdMarker(ZkClient zkClient, String idroot) {
		if (StringUtils.isBlank(idroot)) {
			idroot = ZkSeqIdMaker.DEFAULT_ID_ROOT;
		}
		final ZkSeqIdMaker idMaker = idMarkerMap.get(idroot);
		if (idMaker != null) {
			return idMaker;
		}

		if (idMarkerMap.size() >= MAX_COUNT) {
			throw new ZkIdMakerException("----idMarkerMap.size超出上限:" + MAX_COUNT);
		}

		idMarkerMap.putIfAbsent(idroot, new ZkSeqIdMaker(zkClient, idroot));
		return idMarkerMap.get(idroot);
	}

	public void clear() {
		idMarkerMap.clear();
	}

	public static ZkSeqIdMakerFactory getInstance() {
		return instance;
	}

}
