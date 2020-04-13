package com.tvl.midl.zkclient.exception;

public class ZkClientException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ZkClientException() {
		super();
	}

	public ZkClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ZkClientException(String message, Throwable cause) {
		super(message, cause);
	}

	public ZkClientException(String message) {
		super(message);
	}

	public ZkClientException(Throwable cause) {
		super(cause);
	}

}
