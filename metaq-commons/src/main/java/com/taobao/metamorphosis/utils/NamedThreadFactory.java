package com.taobao.metamorphosis.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 描述：线程的Factory类，以便自定义线程名
 * 
 * @author <a href="mailto:bixuan@taobao.com">bixuan</a>
 */
public class NamedThreadFactory implements ThreadFactory {
    static final AtomicInteger poolNumber = new AtomicInteger(1);
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;


    public NamedThreadFactory() {
        final SecurityManager s = System.getSecurityManager();
        this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
    }


    public NamedThreadFactory(final String name) {
        final SecurityManager s = System.getSecurityManager();
        this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = generateNamePrefix(name);
    }


    public static String generateNamePrefix(final String name) {
        return name + "-" + poolNumber.getAndIncrement() + "-thread-";
    }


    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
     */
    @Override
    public Thread newThread(final Runnable r) {
        // ? 潜在的问题是最后创建的这个线程的计数会不会超过integer的最大值
        final Thread t = new Thread(this.group, r, this.namePrefix + this.threadNumber.getAndIncrement(), 0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
