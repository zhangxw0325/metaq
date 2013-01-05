package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;


public class OffsetCommandUnitTest {
    @Test
    public void testEncode() {
        final OffsetCommand cmd = new OffsetCommand("test", "boyan-test", 1, 1000L, -1);
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("offset test boyan-test 1 1000 -1\r\n", new String(buf.array()));
    }

}
