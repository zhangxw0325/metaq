package com.taobao.metamorphosis.client;

import java.io.Serializable;

import com.taobao.diamond.common.Constants;
import com.taobao.metamorphosis.utils.DiamondUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class MetaClientConfig implements Serializable {
    static final long serialVersionUID = -1L;
    protected String serverUrl;

    /**
     * 从diamond获取zk配置的dataId，默认为"metamorphosis.zkConfig"
     */
    protected String diamondZKDataId = DiamondUtils.DEFAULT_ZK_DATAID;

    /**
     * 从diamond获取zk配置的group，默认为DEFAULT_GROUP
     */
    protected String diamondZKGroup = Constants.DEFAULT_GROUP;

    /**
     * 从diamond获取partitions配置的dataId，默认为"metamorphosis.partitions"
     * */
    private final String diamondPartitionsDataId = DiamondUtils.DEFAULT_PARTITIONS_DATAID;

    /**
     * 从diamond获取partitions配置的group，默认为DEFAULT_GROUP
     */
    private final String diamondPartitionsGroup = Constants.DEFAULT_GROUP;

    protected ZKConfig zkConfig;

    /**
     * recover本地消息的时间间隔
     */
    private long recoverMessageIntervalInMills = 5 * 60 * 1000L;

    private int recoverThreadCount = Runtime.getRuntime().availableProcessors();

    private boolean storeMessageToLocal = false;

    private boolean compressMessage = false;
    private int compressLevel = 9;

    private int producerServerConnectionCount = 1;
    private int consumerServerConnectionCount = 1;
    
    private final String version = "2.1";


    /**
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}
	
	public boolean isVersion2(){
		if(this.version.startsWith("2.")){
			return true;
		}
		return false;
	}


	public boolean isStoreMessageToLocal() {
        return storeMessageToLocal;
    }


    public void setStoreMessageToLocal(boolean storeMessageToLocal) {
        this.storeMessageToLocal = storeMessageToLocal;
    }


    public int getRecoverThreadCount() {
        return this.recoverThreadCount;
    }


    public void setRecoverThreadCount(final int recoverThreadCount) {
        this.recoverThreadCount = recoverThreadCount;
    }


    public long getRecoverMessageIntervalInMills() {
        return this.recoverMessageIntervalInMills;
    }


    public void setRecoverMessageIntervalInMills(final long recoverMessageIntervalInMills) {
        this.recoverMessageIntervalInMills = recoverMessageIntervalInMills;
    }


    public String getDiamondZKDataId() {
        return this.diamondZKDataId;
    }


    public void setDiamondZKDataId(final String diamondZKDataId) {
        this.diamondZKDataId = diamondZKDataId;
    }


    public String getDiamondZKGroup() {
        return this.diamondZKGroup;
    }


    public void setDiamondZKGroup(final String diamondZKGroup) {
        this.diamondZKGroup = diamondZKGroup;
    }


    public ZKConfig getZkConfig() {
        return this.zkConfig;
    }


    public void setZkConfig(final ZKConfig zkConfig) {
        this.zkConfig = zkConfig;
    }


    public String getServerUrl() {
        return this.serverUrl;
    }


    public void setServerUrl(final String serverUrl) {
        this.serverUrl = serverUrl;
    }


    public String getDiamondPartitionsDataId() {
        return this.diamondPartitionsDataId;
    }


    public String getDiamondPartitionsGroup() {
        return this.diamondPartitionsGroup;
    }


    public boolean isCompressMessage() {
        return compressMessage;
    }


    public void setCompressMessage(boolean compressMessage) {
        this.compressMessage = compressMessage;
    }


    public int getCompressLevel() {
        return compressLevel;
    }


    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }


    public int getProducerServerConnectionCount() {
        return producerServerConnectionCount;
    }


    public void setProducerServerConnectionCount(int producerServerConnectionCount) {
        this.producerServerConnectionCount = producerServerConnectionCount;
    }


    public int getConsumerServerConnectionCount() {
        return consumerServerConnectionCount;
    }


    public void setConsumerServerConnectionCount(int consumerServerConnectionCount) {
        this.consumerServerConnectionCount = consumerServerConnectionCount;
    }
}
