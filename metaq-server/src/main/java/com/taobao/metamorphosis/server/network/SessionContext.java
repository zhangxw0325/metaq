package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ConcurrentHashMap;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 会话上下文
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public interface SessionContext {

    public ConcurrentHashMap<TransactionId, Transaction> getTransactions();


    public String getSessionId();


    public Connection getConnection();


    public boolean isInRecoverMode();

}