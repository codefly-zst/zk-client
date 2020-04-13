package com.tvl.midl.zkclient.queue;

public class NodeEntity {
	private String nodePath;

	private String nodeData;

	public NodeEntity(String nodePath, String nodeData) {
		super();
		this.nodePath = nodePath;
		this.nodeData = nodeData;
	}

	public String getNodePath() {
		return nodePath;
	}

	public void setNodePath(String nodePath) {
		this.nodePath = nodePath;
	}

	public String getNodeData() {
		return nodeData;
	}

	public void setNodeData(String nodeData) {
		this.nodeData = nodeData;
	}

}
