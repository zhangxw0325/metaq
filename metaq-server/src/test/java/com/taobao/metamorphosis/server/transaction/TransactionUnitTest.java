package com.taobao.metamorphosis.server.transaction;

import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;

import com.taobao.metamorphosis.transaction.TransactionId;


public abstract class TransactionUnitTest {

    protected TransactionStore transactionStore;

    protected TransactionId xid;


    @Before
    public void setUp() {
        this.transactionStore = EasyMock.createMock(TransactionStore.class);
    }


    @After
    public void tearDown() {
        EasyMock.verify(this.transactionStore);
    }


    protected void mockStoreCommitTwoPhase() throws IOException {
        this.transactionStore.commit(this.xid, true);
        EasyMock.expectLastCall();
    }


    protected void mockStoreCommitOnePhase() throws IOException {
        this.transactionStore.commit(this.xid, false);
        EasyMock.expectLastCall();
    }


    protected void mockStorePrepare() throws IOException {
        this.transactionStore.prepare(this.xid);
        EasyMock.expectLastCall();
    }


    protected void mockStoreRollback() throws IOException {
        this.transactionStore.rollback(this.xid);
        EasyMock.expectLastCall();
    }

}
