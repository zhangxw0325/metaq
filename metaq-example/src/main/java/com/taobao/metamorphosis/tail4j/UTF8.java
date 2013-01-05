package com.taobao.metamorphosis.tail4j;

public class UTF8 {
    /**
     * Helper called by generated code to determine if a byte array is a valid
     * UTF-8 encoded string such that the original bytes can be converted to a
     * String object and then back to a byte array round tripping the bytes
     * without loss.
     * <p>
     * This is inspired by UTF_8.java in sun.nio.cs.
     * 
     * @param byteString
     *            the string to check
     * @return whether the byte array is round trippable
     */
    public static boolean isValidUtf8(byte[] byteString) {
        int index = 0;
        int size = byteString.length;
        // To avoid the masking, we could change this to use bytes;
        // Then X > 0xC2 gets turned into X < -0xC2; X < 0x80
        // gets turned into X >= 0, etc.

        while (index < size) {
            int byte1 = byteString[index++] & 0xFF;
            if (byte1 < 0x80) {
                // fast loop for single bytes
                continue;

                // we know from this point on that we have 2-4 byte forms
            }
            else if (byte1 < 0xC2 || byte1 > 0xF4) {
                // catch illegal first bytes: < C2 or > F4
                return false;
            }
            if (index >= size) {
                // fail if we run out of bytes
                return false;
            }
            int byte2 = byteString[index++] & 0xFF;
            if (byte2 < 0x80 || byte2 > 0xBF) {
                // general trail-byte test
                return false;
            }
            if (byte1 <= 0xDF) {
                // two-byte form; general trail-byte test is sufficient
                continue;
            }

            // we know from this point on that we have 3 or 4 byte forms
            if (index >= size) {
                // fail if we run out of bytes
                return false;
            }
            int byte3 = byteString[index++] & 0xFF;
            if (byte3 < 0x80 || byte3 > 0xBF) {
                // general trail-byte test
                return false;
            }
            if (byte1 <= 0xEF) {
                // three-byte form. Vastly more frequent than four-byte forms
                // The following has an extra test, but not worth restructuring
                if (byte1 == 0xE0 && byte2 < 0xA0 || byte1 == 0xED && byte2 > 0x9F) {
                    // check special cases of byte2
                    return false;
                }

            }
            else {
                // four-byte form

                if (index >= size) {
                    // fail if we run out of bytes
                    return false;
                }
                int byte4 = byteString[index++] & 0xFF;
                if (byte4 < 0x80 || byte4 > 0xBF) {
                    // general trail-byte test
                    return false;
                }
                // The following has an extra test, but not worth restructuring
                if (byte1 == 0xF0 && byte2 < 0x90 || byte1 == 0xF4 && byte2 > 0x8F) {
                    // check special cases of byte2
                    return false;
                }
            }
        }
        return true;
    }
}
