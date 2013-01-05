package com.taobao.metamorphosis.tools.utils;

import java.util.Date;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç5:02:13
 */

public class JvmMemoryUtil {

    public static MonitorResult getMemoryInfo(String ip, int port) throws Exception {
        JMXClient jmxClient = JMXClient.getJMXClient(ip, port);
        ObjectName objectName = new ObjectName("java.lang:type=Memory");
        CompositeData memoryInfo = (CompositeData) jmxClient.getAttribute(objectName, "HeapMemoryUsage");
        jmxClient.close();
        double max = Double.valueOf(memoryInfo.get("max").toString());
        double used = Double.valueOf(memoryInfo.get("used").toString());
        double usedPercent = (used / max) * 100;
        MonitorResult oneResult = new MonitorResult();
        oneResult.setDescribe("");
        oneResult.setKey(ConsoleConstant.MEM);
        oneResult.setIp(ip);
        oneResult.setTime(new Date());
        oneResult.setValue(usedPercent);
        return oneResult;
    }
}
