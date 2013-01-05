package com.taobao.metamorphosis.client;

import org.I0Itec.zkclient.ZkClient;


/**
 * ZkClient变更监听器
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public interface ZkClientChangedListener {
    /**
     * 当新的zkClient建立的时候
     * 
     * @param newClient
     */
    public void onZkClientChangedBefore(ZkClient newClient);
    
    public void onZkClientChanged(ZkClient newClient);
}
