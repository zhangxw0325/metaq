package com.taobao.metamorphosis.tools.monitor;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.monitor.alert.Alarm;


/**
 * 监控启动器
 * 
 * @author 无花
 * @since 2011-5-30 上午11:21:08
 */

public class MonitorStartup {
    private static Logger logger = Logger.getLogger(MonitorStartup.class);


    public static void main(String[] args) {
        ProberManager proberManager = new ProberManager();

        try {
            String source = null;
            proberManager.initProbers(source);
        }
        catch (InitException e) {
            logger.error("fail to startup", e);
            System.exit(-1);
        }

        try {
            proberManager.startProb();
        }
        catch (Throwable e) {
            logger.error("监控系统意外终止", e);
            Alarm.alert("监控系统意外终止", proberManager.getMonitorConfig());
        }
    }

}
