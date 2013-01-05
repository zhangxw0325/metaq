package com.taobao.metamorphosis.tools.utils;

import java.util.Date;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç1:45:30
 */

public class CPULoadUtil {

    public static MonitorResult getCpuLoad(String ip, String user, String password) {
        SSHSupport support = SSHSupport.newInstance(user, password, ip);
        String result = support.execute(ConsoleConstant.CPU_LOAD_CMD);
        double load = getAvarageLoad(result);
        MonitorResult oneResult = new MonitorResult();
        oneResult.setDescribe("");
        oneResult.setIp(ip);
        oneResult.setKey(ConsoleConstant.CPU_LOAD);
        oneResult.setTime(new Date());
        oneResult.setValue(load);

        return oneResult;
    }


    private static double getAvarageLoad(String loadStr) {
        if (loadStr == null || loadStr.indexOf("load average:") == -1) {
            return 0.00;
        }
        int index = loadStr.indexOf("load average:");
        String subStr = loadStr.substring(index + 14);
        int subIndex = subStr.indexOf(',');
        String valueStr = subStr.substring(0, subIndex).trim();
        if (valueStr != null) {
            return Double.valueOf(valueStr);
        }
        return 0.00;
    }


    public static void main(String[] args) {
        System.out.println(CPULoadUtil
            .getAvarageLoad("16:37:15 up 469 days, 22:43, 53 users,  load average: 0.50, 0.00, 0.00"));
    }
}
