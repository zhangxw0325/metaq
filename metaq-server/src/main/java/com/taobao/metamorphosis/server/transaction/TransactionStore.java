package com.taobao.metamorphosis.server.transaction;

import java.io.IOException;

import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.store.MessageStore;
import com.taobao.metamorphosis.server.transaction.store.JournalLocation;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * 事务性存储引擎
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-23
 * 
 */
public interface TransactionStore extends Service {

    void prepare(TransactionId txid) throws IOException;


    void commit(TransactionId txid, boolean wasPrepared) throws IOException;


    void rollback(TransactionId txid) throws IOException;


    public void addMessage(final MessageStore store, long msgId, final PutCommand cmd, JournalLocation location)
            throws IOException;


    void recover(TransactionRecoveryListener listener) throws IOException;
}
