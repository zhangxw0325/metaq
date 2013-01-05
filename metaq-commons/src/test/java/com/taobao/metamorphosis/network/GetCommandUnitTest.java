package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;


public class GetCommandUnitTest {
    @Test
    public void testEncode() {
        final GetCommand cmd = new GetCommand("test", "boyan-group", 1, 1000L, 1024 * 1024, -3);
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("get test boyan-group 1 1000 1048576 -3\r\n", new String(buf.array()));
    }
}
