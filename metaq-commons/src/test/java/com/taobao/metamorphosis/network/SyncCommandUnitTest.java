package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;


public class SyncCommandUnitTest {

    @Test
    public void testEncode() {
        final SyncCommand putCommand = new SyncCommand("test", 1, "hello".getBytes(), 9999L, 0, 0);
        final IoBuffer buf = putCommand.encode();
        assertEquals(0, buf.position());
        assertEquals("sync test 1 5 0 9999 0\r\nhello", new String(buf.array()));
    }

}
