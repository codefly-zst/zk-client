package com.tvl.midl.zkclient.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IP工具�?
 * 
 * @description
 * @author st.z
 */
public class IpUtil {
	private static final Logger LOGGER = LoggerFactory.getLogger(IpUtil.class);
	private static final String ANYHOST = "0.0.0.0";
	private static final String LOCALHOST = "127.0.0.1";
	private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");

	private static InetAddress getInetAddress() {
		InetAddress address;
		try {
			address = InetAddress.getLocalHost();
			return isValidAddress(address) ? address : null;
		} catch (UnknownHostException e) {
			throw new RuntimeException("----获取IP失败");
		}
	}

	public static String getIp() {
		final InetAddress address = getInetAddress();
		if (address == null) {
			return null;
		}
		return address.getHostAddress();
	}

	public static int getValidPort(int port) {
		if (checkPort(port)) {
			return port;
		}
		int tempPort = port;
		int newPort = 0;
		while (++tempPort < 65535) {
			if (checkPort(tempPort)) {
				newPort = tempPort;
				break;
			}
		}

		if (newPort == 0) {
			while (--port > 0) {
				if (checkPort(port)) {
					newPort = port;
					break;
				}
			}
		}

		if (newPort > 0) {
			LOGGER.warn("----port:{}已占�?,绑定新端�?:{}", port, newPort);
			return newPort;
		}

		throw new RuntimeException("----获取本机可用端口失败!");
	}

	private static boolean checkPort(final int port) {
		if (port <= 0 || port > 65535) {
			return false;
		}
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(port);
			return true;
		} catch (IOException e) {
			return false;
		} finally {
			if (ss != null) {
				try {
					ss.close();
				} catch (IOException e) {
					LOGGER.error(e.toString());
				}
			}
		}
	}

	private static boolean isValidAddress(InetAddress address) {
		if (address == null || address.isLoopbackAddress())
			return false;
		final String name = address.getHostAddress();
		return name != null && !ANYHOST.equals(name) && !LOCALHOST.equals(name) && IP_PATTERN.matcher(name).matches();
	}

}
