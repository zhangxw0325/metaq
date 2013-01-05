package com.taobao.metamorphosis.server.transaction.store;

/**
 * 事务引擎MBean接口
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-25
 * 
 */
public interface JournalTransactionStoreMBean {
    /**
     * 执行checkpoint
     * 
     * @throws Exception
     */
    public void makeCheckpoint() throws Exception;


    /**
     * 返回当前活跃事务数
     * 
     * @return
     */
    public int getActiveTransactionCount();

}
