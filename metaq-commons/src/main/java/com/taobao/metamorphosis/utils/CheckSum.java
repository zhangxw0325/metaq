package com.taobao.metamorphosis.utils;

import java.util.zip.CRC32;


/**
 * Checksum¼ÆËãÆ÷
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class CheckSum {
    public static final int crc32(byte[] array) {
        return crc32(array, 0, array.length);
    }


    public static final int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }
}
