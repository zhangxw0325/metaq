package com.taobao.metamorphosis.utils.test;

import java.util.concurrent.CyclicBarrier;


/**
 * 
 * 并发测试案例
 * 
 * @author boyan
 * 
 * @since 1.0, 2010-1-11 下午03:14:28
 */

public class ConcurrentTestCase {
    private int threadCount;
    private final int repeatCount;

    private CyclicBarrier barrier;

    private ConcurrentTestTask task;

    private final ClockWatch watch = new ClockWatch();


    public ConcurrentTestCase(int threadCount, int repeatCount, ConcurrentTestTask task) {
        super();
        this.threadCount = threadCount;
        this.repeatCount = repeatCount;
        this.task = task;
    }


    public ConcurrentTestCase(int threadCount, ConcurrentTestTask task) {
        super();
        this.threadCount = threadCount;
        this.repeatCount = 1;
        this.task = task;
    }


    public void start() {
        this.barrier = new CyclicBarrier(this.threadCount + 1, this.watch);
        for (int i = 0; i < this.threadCount; i++) {
            new Thread(new ConcurrentTestRunner(this.barrier, this.task, this.repeatCount, i)).start();
        }
        try {
            this.watch.start();
            this.barrier.await();
            this.barrier.await();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public int getThreadCount() {
        return this.threadCount;
    }


    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }


    public ConcurrentTestTask getTask() {
        return this.task;
    }


    public void setTask(ConcurrentTestTask task) {
        this.task = task;
    }


    public long getDurationInMillis() {
        return this.watch.getDurationInMillis();
    }


    public long getDurationInNano() {
        return this.watch.getDurationInNano();
    }

}
