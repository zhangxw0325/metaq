package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.TransactionInfo;
import com.taobao.metamorphosis.transaction.TransactionInfo.TransactionType;


public class TransactionCommandUnitTest {

    @Test
    public void testEncode() {
        final TransactionId id = new LocalTransactionId("sessionId", 99);
        final TransactionInfo info = new TransactionInfo(id, "sessionId", TransactionType.COMMIT_ONE_PHASE);

        final TransactionCommand cmd = new TransactionCommand(info, 100);
        final IoBuffer buf = cmd.encode();
        assertEquals("transaction TX:sessionId:99 sessionId COMMIT_ONE_PHASE 100\r\n", new String(buf.array()));
    }


    @Test
    public void testEncodeWithTimeout() {
        final TransactionId id = new LocalTransactionId("sessionId", 99);
        final TransactionInfo info = new TransactionInfo(id, "sessionId", TransactionType.BEGIN, 3);

        final TransactionCommand cmd = new TransactionCommand(info, 100);
        final IoBuffer buf = cmd.encode();
        assertEquals("transaction TX:sessionId:99 sessionId BEGIN 3 100\r\n", new String(buf.array()));
    }
}
