package com.taobao.metamorphosis.server.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * 系统时间缓存
 * 
 * @author boyan
 * @Date 2010-9-28
 * 
 */
public class SystemTimer {
    private final static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private static final long tickUnit = Long.parseLong(System.getProperty("notify.systimer.tick", "50"));

    static {
        executor.scheduleAtFixedRate(new TimerTicker(), tickUnit, tickUnit, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                executor.shutdown();
            }
        });
    }

    private static volatile long time = System.currentTimeMillis();

    private static class TimerTicker implements Runnable {
        public void run() {
            time = System.currentTimeMillis();
        }
    }


    public static long currentTimeMillis() {
        return time;
    }

}
