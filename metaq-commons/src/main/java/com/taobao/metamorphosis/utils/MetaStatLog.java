package com.taobao.metamorphosis.utils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.monitor.MonitorLog;


/**
 * 
 * 
 * StatLog的包装类
 * 
 * @author boyan
 * 
 * @since 1.0, 2009-6-2 下午03:15:37
 */

public final class MetaStatLog {

    public static boolean startRealTimeStat = false;


    private MetaStatLog() {

    }

    // 没有任何作用，仅是为了注册监听器
    static MetaStatLog ME = new MetaStatLog();

    static ConcurrentHashMap<String/* key1 */, ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>>> realTimeStatMap =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>>>();

    public static volatile long lastResetTime = System.currentTimeMillis();

    static class StatCounter {
        private final AtomicLong count = new AtomicLong(0L);
        private final AtomicLong value = new AtomicLong(0L);


        public void incrementCount() {
            this.count.incrementAndGet();
        }


        public void addValue(long value) {
            this.value.addAndGet(value);
        }


        public synchronized void reset() {
            this.count.set(0L);
            this.value.set(0L);
        }
    }


    public static void clearRealTimeStat() {
        realTimeStatMap =
                new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>>>();
    }

    public static class RealTimeStatRestTask implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(30 * 60 * 1000);

                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                resetRealTimeStat();

            }
        }
    }


    public synchronized static final void resetRealTimeStat() {
        for (Map.Entry<String, ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>>> entry1 : realTimeStatMap
            .entrySet()) {
            for (Map.Entry<String, ConcurrentHashMap<String, StatCounter>> entry2 : entry1.getValue().entrySet()) {
                for (Map.Entry<String, StatCounter> entry3 : entry2.getValue().entrySet()) {
                    entry3.getValue().reset();
                }
            }
        }
        lastResetTime = System.currentTimeMillis();
    }


    public static List<String> getRealTimeStatItemNames() {
        List<String> result = new ArrayList<String>();
        for (Map.Entry<String, ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>>> entry1 : realTimeStatMap
            .entrySet()) {
            for (Map.Entry<String, ConcurrentHashMap<String, StatCounter>> entry2 : entry1.getValue().entrySet()) {
                for (Map.Entry<String, StatCounter> entry3 : entry2.getValue().entrySet()) {
                    StringBuilder sb = new StringBuilder("[");
                    sb.append(entry1.getKey()).append(",");
                    sb.append(entry2.getKey()).append(",");
                    sb.append(entry3.getKey()).append("]");
                    result.add(sb.toString());
                }
            }
        }
        return result;
    }


    public static final String getRealTimeStatResult(String key1, String key2, String key3) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>> map1 = realTimeStatMap.get(key1);
        if (map1 == null) {
            return "Invalid keyOne:" + key1;
        }
        ConcurrentHashMap<String, StatCounter> map2 = map1.get(key2);
        if (map2 == null) {
            return "Invalid keyTwo:" + key2;
        }
        StatCounter counter = map2.get(key3);
        if (counter == null) {
            return "Invalid keyThree:" + key3;
        }
        return formatOutput(counter);
    }


    private static String formatOutput(StatCounter counter) {
        double count = counter.count.get();
        double values = counter.value.get();
        long duration = (System.currentTimeMillis() - lastResetTime) / 1000;
        String averageValueStr = "invalid";
        String averageCountStr = "invalid";
        DecimalFormat numberFormat = new DecimalFormat("#.##");
        if (count != 0) {
            double averageValue = values / count;
            averageValueStr = numberFormat.format(averageValue);
        }
        if (duration != 0) {
            double averageCount = count / duration;
            averageCountStr = numberFormat.format(averageCount);
        }
        return String.format(OUTPUT_FORMAT, numberFormat.format(count), numberFormat.format(values), averageValueStr,
            averageCountStr, duration);
    }


    public static final String getGroupedRealTimeStatResult(String key1) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>> map1 = realTimeStatMap.get(key1);
        if (null == map1) {
            return null;
        }
        StatCounter statCounter = new StatCounter();
        for (Map.Entry<String/* key2 */, ConcurrentHashMap<String/* key3 */, StatCounter>> entry1 : map1.entrySet()) {
            ConcurrentHashMap<String/* key3 */, StatCounter> map2 = entry1.getValue();
            if (null == map2) {
                continue;
            }
            for (Map.Entry<String, StatCounter> entry2 : map2.entrySet()) {
                statCounter.count.addAndGet(entry2.getValue().count.longValue());
                statCounter.value.addAndGet(entry2.getValue().value.longValue());
            }
        }

        return formatOutput(statCounter);
    }


    public static final String getGroupedRealTimeStatResult(String key1, String key2) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>> map1 = realTimeStatMap.get(key1);
        if (null == map1) {
            return null;
        }
        ConcurrentHashMap<String, StatCounter> map2 = map1.get(key2);
        if (map2 == null) {
            return "Invalid keyTwo:" + key2;
        }

        StatCounter statCounter = new StatCounter();
        for (Map.Entry<String, StatCounter> entry2 : map2.entrySet()) {
            statCounter.count.addAndGet(entry2.getValue().count.longValue());
            statCounter.value.addAndGet(entry2.getValue().value.longValue());
        }

        return formatOutput(statCounter);
    }


    public static long getDuration() {
        return (System.currentTimeMillis() - lastResetTime) / 1000;
    }

    public static String OUTPUT_FORMAT = "Count=%s,Value=%s,Value/Count=%s,Count/Duration=%s,Duration=%d";


    public static final void addStat(String appName, String keyOne, String keyTwo, String keyThree) {
        realTimeStat(keyOne, keyTwo, keyThree, 0);
        MonitorLog.addStat(keyOne, keyTwo, keyThree, 0L, 1L);
    }

    @SuppressWarnings("unused")
    private static class RealTimeStaticKey {

        private String key1;
        private String key2;
        private String key3;


        public RealTimeStaticKey(String key1, String key2, String key3) {
            this.key1 = key1;
            this.key2 = key2;
            this.key3 = key3;
        }


        public String getKey1() {
            return this.key1;
        }


        public void setKey1(String key1) {
            this.key1 = key1;
        }


        public String getKey2() {
            return this.key2;
        }


        public void setKey2(String key2) {
            this.key2 = key2;
        }


        public String getKey3() {
            return this.key3;
        }


        public void setKey3(String key3) {
            this.key3 = key3;
        }

    }


    public static final void realTimeStat(String key1, String key2, String key3, long value) {
        if (startRealTimeStat) {
            processMap2(key1, key2, key3, value);
        }
    }


    private static void processMap2(String key1, String key2, String key3, long value) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>> statMap1 = realTimeStatMap.get(key1);
        if (statMap1 == null) {
            statMap1 = new ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>>();
            ConcurrentHashMap<String, ConcurrentHashMap<String, StatCounter>> oldStatMap1 =
                    realTimeStatMap.putIfAbsent(key1, statMap1);
            if (oldStatMap1 != null) {
                statMap1 = oldStatMap1;
            }
        }
        ConcurrentHashMap<String, StatCounter> statMap2 = statMap1.get(key2);
        if (statMap2 == null) {
            statMap2 = new ConcurrentHashMap<String, StatCounter>();
            ConcurrentHashMap<String, StatCounter> oldStatMap2 = statMap1.putIfAbsent(key2, statMap2);
            if (oldStatMap2 != null) {
                statMap2 = oldStatMap2;
            }
        }

        StatCounter statCounter = statMap2.get(key3);
        if (statCounter == null) {
            statCounter = new StatCounter();
            StatCounter oldCounter = statMap2.putIfAbsent(key3, statCounter);
            if (oldCounter != null) {
                statCounter = oldCounter;
            }
        }
        statCounter.incrementCount();
        statCounter.addValue(value);
    }


    public static final void addStat(String appName, String keyOne, String keyTwo) {
        realTimeStat(keyOne, keyTwo, "*", 0);

        MonitorLog.addStat(keyOne, keyTwo, null, 0L, 1L);
    }


    public static final void addStat(String appName, String keyOne) {

        realTimeStat(keyOne, "*", "*", 0);
        MonitorLog.addStat(keyOne, null, null, 0L, 1L);
    }


    public static final void addStatValue2(String appName, String keyOne, long value) {
        realTimeStat(keyOne, "*", "*", value);

        MonitorLog.addStat(keyOne, null, null, value, 1L);
    }


    public static final void addStatValue2(String appName, String keyOne, String keyTwo, long value) {
        realTimeStat(keyOne, keyTwo, "*", value);

        MonitorLog.addStatValue2(keyOne, keyTwo, null, value);
    }


    public static final void addStatValue2(String appName, String keyOne, String keyTwo, String keyThree, long value) {
        realTimeStat(keyOne, keyTwo, keyThree, value);

        MonitorLog.addStat(keyOne, keyTwo, keyThree, value, 1L);
    }

}
