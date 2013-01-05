/**
 * $Id: SystemClock.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.commons;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * https://github.com/zhongl/jtoolkit/blob/master/common/src/main/java/com/github/zhongl/jtoolkit/SystemClock.java
 */
public class SystemClock {

    private final long precision;
    private final AtomicLong now;


    public SystemClock(long precision) {
        this.precision = precision;
        now = new AtomicLong(System.currentTimeMillis());
        scheduleClockUpdating();
    }


    private void scheduleClockUpdating() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "System Clock");
                thread.setDaemon(true);
                return thread;
            }
        });
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                now.set(System.currentTimeMillis());
            }
        }, precision, precision, TimeUnit.MILLISECONDS);
    }


    public long now() {
        return now.get();
    }


    public long precision() {
        return precision;
    }
}
