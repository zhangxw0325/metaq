package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;


public class StatsCommandUnitTest {
    @Test
    public void testEncodeEmptyItem() {
        final StatsCommand cmd = new StatsCommand(-1000, null);
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("stats  -1000\r\n", new String(buf.array()));
    }


    @Test
    public void testEncodeWithItem() {
        final StatsCommand cmd = new StatsCommand(-1000, "topics");
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("stats topics -1000\r\n", new String(buf.array()));
    }

}
