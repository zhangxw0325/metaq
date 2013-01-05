package com.taobao.metamorphosis.metaslave;

import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.utils.MetaStatLog;


/**
 * 
 * @author 无花
 * @since 2011-11-9 上午10:35:17
 */

public class SlaveStatsManager {

    /**
     * slave服务端put
     */
    public static final String SLAVE_CMD_PUT = "put_slave";

    private final StatsManager statsManager;


    public SlaveStatsManager(StatsManager statsManager) {
        this.statsManager = statsManager;
    }


    public void statsSlavePut(final String topic, String partition, final int c) {
        this.statsManager.statsRealtimePut(c);
        MetaStatLog.addStatValue2(null, SLAVE_CMD_PUT, topic, partition, c);
    }


    public void statsMessageSize(String topic, int length) {
        this.statsManager.statsMessageSize(topic, length);
    }


    public void statsSlavePutFailed(String topic, String partition, int c) {
        this.statsManager.statsPutFailed(topic, partition, c);
    }
}
