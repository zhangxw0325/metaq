package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;


public class PutCommandUnitTest {

    @Test
    public void testEncode_HasTransactionId() {
        final TransactionId id = new LocalTransactionId("test", 1);
        final PutCommand putCommand = new PutCommand("test", 1, "hello".getBytes(), id, 0, 0);
        final IoBuffer buf = putCommand.encode();
        assertEquals(0, buf.position());
        assertEquals("put test 1 5 0 TX:test:1 0\r\nhello", new String(buf.array()));
    }


    @Test
    public void testEncode_NoTransactionId() {
        final PutCommand putCommand = new PutCommand("test", 1, "hello".getBytes(), null, 0, 0);
        final IoBuffer buf = putCommand.encode();
        assertEquals(0, buf.position());
        assertEquals("put test 1 5 0 0\r\nhello", new String(buf.array()));
    }
}
