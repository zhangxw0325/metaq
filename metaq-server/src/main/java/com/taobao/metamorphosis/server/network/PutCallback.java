package com.taobao.metamorphosis.server.network;

import com.taobao.gecko.core.command.ResponseCommand;

/**
 * Put消息的回调接口
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public interface PutCallback {

    public void putComplete(ResponseCommand resp);
}
