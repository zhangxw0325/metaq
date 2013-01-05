/**
 * $Id: TestMix.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.research;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Time;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


public class TestMix {
    private static void Test1(String[] args) {
        int intA = 65535;
        short shortA = (short) intA;
        int intB = (shortA & 0xffff);
        System.out.println(shortA);
        System.out.println(intB);
    }


    private static void Test2(String[] args) {
        String string = "MESSAGE_TYPE_ABCde";
        System.out.println(string.hashCode());
    }


    private static void Test3(String[] args) throws UnknownHostException {
        // ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4);
        // byteBuffer.putInt((int) (System.currentTimeMillis() / 1000));
        //
        // InetAddress inet = InetAddress.getLocalHost();
        // byteBuffer.put(inet.getAddress());
        // System.out.println(bytes2string(byteBuffer.array()));
        // System.out.println(inet.getAddress().length);

    }


    private static void Test4(String[] args) throws IOException {
        File file = new File("/data");
        System.out.println("getAbsolutePath " + file.getAbsolutePath());
        System.out.println("getCanonicalPath " + file.getCanonicalPath());
        System.out.println("getName " + file.getName());
        System.out.println("getPath " + file.getPath());
        System.out.println("getParent " + file.getParent());
        System.out.println("getTotalSpace " + file.getTotalSpace());
        System.out.println("getUsableSpace " + file.getUsableSpace());
        System.out.println("getFreeSpace " + file.getFreeSpace());

        System.out.println("getUsableSpace RATIO " + (file.getUsableSpace() - file.getFreeSpace())
                / file.getUsableSpace());
        System.out.println("getTotalSpace RATIO " + (file.getTotalSpace() - file.getFreeSpace())
                / file.getTotalSpace());

    }


    private static long retryInterval() {
        long result = 5000;

        result += (Math.random() * 10000) % result;

        return result;
    }


    private static void Test5(String[] args) throws Exception {

        for (int i = 0;; i++) {
            System.out.println(retryInterval());
            Thread.sleep(1000);
        }
    }


    private static void Test6(String[] args) throws Exception {
        long lastTime = System.nanoTime();
        for (long i = 0;; i++) {
            long start = System.nanoTime();
            long end = System.nanoTime();

            if ((i % 1000000) == 0) {
                long eclipseTime = System.nanoTime() - lastTime;
                System.out.println(eclipseTime);
                lastTime = System.nanoTime();
            }
        }
    }


    private static void Test7(String[] args) throws Exception {
        try {
            return;
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally {
            System.out.println("a");
        }
    }


    private static void Test8(String[] args) throws Exception {
        long a = 17;
        System.out.println(Math.ceil(a / 17.0d));
    }


    private static void Test9(String[] args) throws Exception {
        Time t = new Time(System.currentTimeMillis());
        Date d = new Date();
    }


    private static void Test10(String[] args) throws Exception {
        final AtomicLong[] putMessageDistributeTime = new AtomicLong[6];

        for (int i = 0; i < putMessageDistributeTime.length; i++) {
            putMessageDistributeTime[i] = new AtomicLong(i);
        }

        System.out.println(putMessageDistributeTime);

    }


    private static void Test11(String[] args) throws Exception {
        System.out.println(Integer.toHexString(MessageMagicCode));
        System.out.println(Integer.toHexString(BlankMagicCode));
    }


    private static void Test12(String[] args) throws Exception {
        Properties propertie = new Properties();

        try {
            FileInputStream inputFile = new FileInputStream("d:\\a.pp");
            propertie.load(inputFile);
            inputFile.close();
        }
        catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }

        String zkRootPath = propertie.getProperty("zkRootPath", "/metaqdefault");
        // System.out.println("[" + zkRootPath + "]");
        Set<String> keys = propertie.stringPropertyNames();
        System.out.println(keys);
        // propertie.load(inStream)
    }


    private static void Test13(String[] args) throws Exception {
        // Properties propertie = System.getProperties();
        // Set<String> keys = propertie.stringPropertyNames();
        // for (String key : keys) {
        // System.out.println(key + "\t==" + propertie.getProperty(key));
        // }

        System.out.println(InetAddress.getLocalHost().getHostName());
    }

    private final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;
    // 文件末尾空洞对应的MAGIC CODE
    private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;


    public static void main(String[] args) throws Exception {

        Test13(args);
    }
}
