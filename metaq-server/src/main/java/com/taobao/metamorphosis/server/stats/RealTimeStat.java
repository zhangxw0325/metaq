package com.taobao.metamorphosis.server.stats;

import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.utils.MetaStatLog;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-9-16 上午11:56:00
 */

public class RealTimeStat implements RealTimeStatMBean {

    private static final Logger log = Logger.getLogger(RealTimeStat.class);
    private Thread resetTask;


    public RealTimeStat() {
        // TODO 暴露JMX
    }


    public void start() {
        this.resetTask = new Thread(new MetaStatLog.RealTimeStatRestTask());
        this.resetTask.start();
        log.warn("实时统计启动...");
    }


    public void stop() {
        this.resetTask.interrupt();
        while (this.resetTask.isAlive()) {
            try {
                this.resetTask.join();
            }
            catch (InterruptedException e) {

            }
        }
        log.warn("实时统计关闭...");
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.notify.utils.RealTimeNotifyStatMBean#getRealTimeStatKeys()
     */
    @Override
    public List<String> getRealTimeStatItemNames() {
        return MetaStatLog.getRealTimeStatItemNames();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.notify.utils.RealTimeNotifyStatMBean#resetStat()
     */
    @Override
    public void resetStat() {
        MetaStatLog.resetRealTimeStat();
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.taobao.notify.utils.RealTimeNotifyStatMBean#getStatDuration()
     */
    @Override
    public long getStatDuration() {
        return (System.currentTimeMillis() - MetaStatLog.lastResetTime) / 1000;
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.notify.utils.RealTimeNotifyStatMBean#getStatResult(java.lang
     * .String, java.lang.String, java.lang.String)
     */
    @Override
    public String getStatResult(String key1, String key2, String key3) {
        return MetaStatLog.getRealTimeStatResult(key1, key2, key3);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.notify.utils.RealTimeNotifyStatMBean#getStatResult(java.lang
     * .String, java.lang.String)
     */
    @Override
    public String getStatResult(String key1, String key2) {
        return MetaStatLog.getRealTimeStatResult(key1, key2, "*");
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.notify.utils.RealTimeNotifyStatMBean#getStatResult(java.lang
     * .String)
     */
    @Override
    public String getStatResult(String key1) {
        return MetaStatLog.getRealTimeStatResult(key1, "*", "*");
    }


    @Override
    public String getGroupedRealTimeStatResult(String key1) {
        return MetaStatLog.getGroupedRealTimeStatResult(key1);
    }


    public String getGroupedRealTimeStatResult(String key1, String key2) {
        return MetaStatLog.getGroupedRealTimeStatResult(key1, key2);
    }


    @Override
    public long getDuration() {
        return MetaStatLog.getDuration();
    }
}
