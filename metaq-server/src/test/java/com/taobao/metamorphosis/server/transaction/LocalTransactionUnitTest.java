package com.taobao.metamorphosis.server.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAException;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;


public class LocalTransactionUnitTest extends TransactionUnitTest {

    private LocalTransaction localTransaction;
    private SessionContext context;
    private ConcurrentHashMap<TransactionId, Transaction> txMap;


    @Override
    @Before
    public void setUp() {
        super.setUp();
        this.txMap = new ConcurrentHashMap<TransactionId, Transaction>();
        this.xid = new LocalTransactionId("test", 99);
        this.context = EasyMock.createMock(SessionContext.class);
        EasyMock.expect(this.context.getTransactions()).andReturn(this.txMap).anyTimes();
        this.localTransaction =
                new LocalTransaction(this.transactionStore, (LocalTransactionId) this.xid, this.context);
        this.txMap.put(this.xid, this.localTransaction);

    }


    private void replay() {
        EasyMock.replay(this.transactionStore, this.context);
    }


    @Test(expected = XAException.class)
    public void testPrepare() throws Exception {
        this.replay();
        this.localTransaction.prepare();
    }


    @Test
    public void testCommit() throws Exception {
        this.mockStoreCommitOnePhase();
        this.replay();
        this.localTransaction.commit(true);
        assertEquals(Transaction.FINISHED_STATE, this.localTransaction.getState());
        assertNull(this.txMap.get(this.xid));
    }


    @Test(expected = XAException.class)
    public void testCommitPrepared() throws Exception {
        this.localTransaction.setState(Transaction.PREPARED_STATE);
        this.replay();
        this.localTransaction.commit(true);
    }


    @Test
    public void testRollback() throws Exception {
        this.mockStoreRollback();
        this.replay();
        this.localTransaction.rollback();
        assertEquals(Transaction.FINISHED_STATE, this.localTransaction.getState());
        assertNull(this.txMap.get(this.xid));
    }


    @Override
    @After
    public void tearDown() {
        super.tearDown();
        EasyMock.verify(this.context);
    }
}
