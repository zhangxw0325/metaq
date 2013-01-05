package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;


public class BooleanCommandUnitTest {
    @Test
    public void testEncodeWithMessage() {
        final BooleanCommand cmd = new BooleanCommand(99, HttpStatus.NotFound, "not found");
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("result 404 9 99\r\nnot found", new String(buf.array()));
    }


    @Test
    public void testEncodeWithoutMessage() {
        final BooleanCommand cmd = new BooleanCommand(99, HttpStatus.NotFound, null);
        final IoBuffer buf = cmd.encode();
        assertEquals(0, buf.position());
        assertEquals("result 404 0 99\r\n", new String(buf.array()));
    }
}
