package com.taobao.metamorphosis.utils;

import static org.junit.Assert.*;

import org.junit.Test;


public class CheckSumUnitTest {
    @Test
    public void testCheckSum() throws Exception {
        byte[] data2 = new byte[1024];
        byte[] data1 = new byte[1024];
        for (int i = 0; i < data1.length; i++) {
            data1[i] = (byte) (i % 127);
            data2[i] = (byte) (i % 127);
        }
        assertEquals(CheckSum.crc32(data1), CheckSum.crc32(data1));
        assertEquals(CheckSum.crc32(data2), CheckSum.crc32(data2));
        assertEquals(CheckSum.crc32(data1), CheckSum.crc32(data2));
    }
}
