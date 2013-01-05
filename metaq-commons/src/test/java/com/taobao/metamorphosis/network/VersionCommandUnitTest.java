package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;


public class VersionCommandUnitTest {
    @Test
    public void testEncode() {
        final VersionCommand cmd = new VersionCommand(999);
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("version 999\r\n", new String(buf.array()));
    }
}
