package com.taobao.metamorphosis.tools.utils;

import java.util.Date;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç3:21:25
 */

public class DiskUsedUtil {

    private DiskUsedUtil() {
    }


    public static MonitorResult getDiskUsed(String ip, String user, String password) {
        SSHSupport support = SSHSupport.newInstance(user, password, ip);
        String result = support.execute(ConsoleConstant.DISK_CMD);
        double used = getDiskUsed(result);
        MonitorResult oneResult = new MonitorResult();
        oneResult.setDescribe("");
        oneResult.setKey(ConsoleConstant.DISK);
        oneResult.setIp(ip);
        oneResult.setTime(new Date());
        oneResult.setValue(used);

        return oneResult;
    }


    private static double getDiskUsed(String loadStr) {
        if (loadStr == null || loadStr.indexOf("%") == -1) {
            return 0.00;
        }
        int index = loadStr.indexOf("%");
        String subStr = loadStr.substring(index - 3, index).trim();

        if (subStr != null) {
            return Double.valueOf(subStr);
        }
        return 0.00;
    }


    public static void main(String[] args) {
        System.out.println(DiskUsedUtil.getDiskUsed("/dev/sda9              35G  9.7G   24G  90% /home"));
    }
}
