package com.taobao.metamorphosis.gregor.slave;

import com.taobao.metamorphosis.network.SyncCommand;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;


/**
 * 同步命令处理接口
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public interface SyncCommandProcessor {
    /**
     * 处理同步命令
     * 
     * @param request
     * @param sessionContext
     * @param cb
     */
    public void processSyncCommand(final SyncCommand request, final SessionContext sessionContext, final PutCallback cb);
}
