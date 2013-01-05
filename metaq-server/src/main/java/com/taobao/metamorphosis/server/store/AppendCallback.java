package com.taobao.metamorphosis.server.store;

/**
 * Append回调
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public interface AppendCallback {

    /**
     * 在append成功后回调此方法，传入写入的location
     * 
     * @param location
     */
    public void appendComplete(Location location);
}
