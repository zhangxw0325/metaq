package com.taobao.metamorphosis.utils.test;

import java.util.concurrent.CyclicBarrier;


/**
 * 
 * 
 * 
 * @author boyan
 * 
 * @since 1.0, 2010-1-11 ÏÂÎç03:12:41
 */

public class ConcurrentTestRunner implements Runnable {
    private CyclicBarrier barrier;

    private ConcurrentTestTask task;

    private int repeatCount;

    private int index;


    public ConcurrentTestRunner(CyclicBarrier barrier, ConcurrentTestTask task, int repeatCount, int index) {
        super();
        this.barrier = barrier;
        this.task = task;
        this.repeatCount = repeatCount;
        this.index = index;
    }


    public void run() {
        try {
            barrier.await();
            for (int i = 0; i < repeatCount; i++) {
                task.run(this.index, i);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                barrier.await();
            }
            catch (Exception e) {
                // ignore
            }
        }
    }
}
