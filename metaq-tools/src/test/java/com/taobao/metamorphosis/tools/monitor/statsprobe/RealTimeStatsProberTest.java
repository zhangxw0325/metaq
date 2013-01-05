package com.taobao.metamorphosis.tools.monitor.statsprobe;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.tools.monitor.statsprobe.RealTimeStatsProber;

/**
 *
 * @author ÎÞ»¨
 * @since 2011-5-27 ÏÂÎç05:48:39
 */

public class RealTimeStatsProberTest {
    
    @Test
    public void testIsNeedAlert() {
        Assert.assertFalse(RealTimeStatsProber.isNeedAlert("realtime_put_failed null", 10));
        Assert.assertTrue(RealTimeStatsProber.isNeedAlert("realtime_put_failed Count=1,Value=10,Value/Count=11,Count/Duration=,Duration=", 10));
        Assert.assertFalse(RealTimeStatsProber.isNeedAlert("realtime_put_failed Count=1,Value=9,Value/Count=11,Count/Duration=,Duration=", 10));
    }

}
