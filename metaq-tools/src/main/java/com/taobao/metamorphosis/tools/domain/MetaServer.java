package com.taobao.metamorphosis.tools.domain;

public class MetaServer implements Comparable<MetaServer>{
	private String hostName;
    private String hostIp;
    private int status;
    private int serverPort;
    private int jmxPort;
    private String cluster;
    private String url;
    private int brokeId;
    
	public int getBrokeId() {
		return brokeId;
	}
	public void setBrokeId(int brokeId) {
		this.brokeId = brokeId;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getHostIp() {
		return hostIp;
	}
	public void setHostIp(String hostIp) {
		this.hostIp = hostIp;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public int getServerPort() {
		return serverPort;
	}
	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}
	public int getJmxPort() {
		return jmxPort;
	}
	public void setJmxPort(int jmxPort) {
		this.jmxPort = jmxPort;
	}
	public String getCluster() {
		return cluster;
	}
	public void setCluster(String cluster) {
		this.cluster = cluster;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public int compareTo(MetaServer o) {
		return this.getUrl().compareTo(o.getUrl());
	}
}
