package com.taobao.metamorphosis.server.stats;

import java.util.List;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-9-16 下午12:00:27
 */

public interface RealTimeStatMBean {

    /**
     * 查看实时统计的key信息
     * 
     * @return
     */
    public  List<String> getRealTimeStatItemNames();


    /**
     * 重新开始实时统计
     */
    public  void resetStat();


    /**
     * 实时统计进行的时间，单位秒
     * 
     * @return
     */
    public  long getStatDuration();


    /**
     * 获取实时统计结果
     * 
     * @param key1
     * @param key2
     * @param key3
     * @return
     */
    public  String getStatResult(String key1, String key2, String key3);


    public  String getStatResult(String key1, String key2);


    public  String getStatResult(String key1);


    public String getGroupedRealTimeStatResult(String key1);


    public long getDuration();
}