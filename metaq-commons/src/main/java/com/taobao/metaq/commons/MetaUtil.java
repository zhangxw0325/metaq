/**
 * $Id: MetaUtil.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;


public class MetaUtil {
    /**
     * 将offset转化成字符串形式<br>
     * 左补零对齐至20位
     */
    public static String Offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }


    /**
     * 计算耗时操作，单位ms
     */
    public static long computeEclipseTimeMilliseconds(final long beginTime) {
        return (System.currentTimeMillis() - beginTime);
    }


    public static boolean isItTimeToDo(final String when) {
        String[] whiles = when.split(";");
        if (whiles != null && whiles.length > 0) {
            Calendar now = Calendar.getInstance();
            for (String w : whiles) {
                int nowHour = Integer.parseInt(w);
                if (nowHour == now.get(Calendar.HOUR_OF_DAY)) {
                    return true;
                }
            }
        }

        return false;
    }


    public static String timeMillisToHumanString(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        Date date = cal.getTime();
        return date.toLocaleString();
    }


    /**
     * 获取磁盘分区空间使用率
     */
    public static double getDiskPartitionSpaceUsedPercent(final String path) {
        if (null == path || path.isEmpty())
            return -1;

        try {
            File file = new File(path);
            if (!file.exists())
                file.mkdirs();

            long totalSpace = file.getTotalSpace();
            long freeSpace = file.getFreeSpace();
            long usedSpace = totalSpace - freeSpace;
            if (totalSpace > 0) {
                return usedSpace / (double) totalSpace;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        return -1;
    }


    public static final int crc32(byte[] array) {
        if (array != null) {
            return crc32(array, 0, array.length);
        }

        return 0;
    }


    public static final int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }


    /**
     * 字节数组转化成16进制形式
     */
    public static String bytes2string(byte[] src) {
        StringBuilder sb = new StringBuilder(40);
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                sb.append(0);
            }
            sb.append(hv.toUpperCase());
        }
        return sb.toString();
    }


    /**
     * 16进制字符串转化成字节数组
     */
    public static byte[] string2bytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }


    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }


    public static byte[] uncompress(final byte[] src) throws IOException {
        byte[] result = src;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            while (true) {
                int len = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                byteArrayOutputStream.write(uncompressData, 0, len);
            }
            byteArrayOutputStream.flush();
            result = byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
        finally {
            try {
                byteArrayInputStream.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            try {
                inflaterInputStream.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            try {
                byteArrayOutputStream.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }


    public static byte[] compress(final byte[] src, final int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        java.util.zip.Deflater deflater = new java.util.zip.Deflater(level);
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, deflater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
            result = byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) {
            deflater.end();
            e.printStackTrace();
            throw e;
        }
        finally {
            try {
                byteArrayOutputStream.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }

            deflater.end();
        }

        return result;
    }
}
