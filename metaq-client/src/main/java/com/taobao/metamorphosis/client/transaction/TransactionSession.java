package com.taobao.metamorphosis.client.transaction;

/**
 * 事务性会话
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-25
 * 
 */
public interface TransactionSession {

    public void removeContext(TransactionContext ctx);


    public String getSessionId();

}
