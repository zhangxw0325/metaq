package com.taobao.metamorphosis.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;


public class HexSupportUnitTest {

    @Test
    public void testToHexFromBytesToBytesFromHex() {
        final byte[] bytes = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            final byte t = (byte) (i % Byte.MAX_VALUE);
            bytes[i] = (byte) (i % 2 == 0 ? t : -t);
        }

        final String s = HexSupport.toHexFromBytes(bytes);
        System.out.println(s);
        assertEquals(s, HexSupport.toHexFromBytes(bytes));
        assertEquals(s, HexSupport.toHexFromBytes(bytes));

        final byte[] decodedBytes = HexSupport.toBytesFromHex(s);
        assertArrayEquals(bytes, decodedBytes);
        assertArrayEquals(bytes, HexSupport.toBytesFromHex(s));
        assertArrayEquals(bytes, HexSupport.toBytesFromHex(s));
    }
}
