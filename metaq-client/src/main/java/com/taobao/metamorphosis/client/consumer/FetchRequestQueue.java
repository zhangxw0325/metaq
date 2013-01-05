package com.taobao.metamorphosis.client.consumer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 稳定排序的delay queue，线程安全
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
class FetchRequestQueue {
    private final LinkedList<FetchRequest> queue = new LinkedList<FetchRequest>();
    private final Lock lock = new ReentrantLock();
    private final Condition available = this.lock.newCondition();
    private volatile boolean shutdown = false;


    public void shutdown() {
        this.shutdown = true;
    }


    public FetchRequest take() throws InterruptedException {
        this.lock.lockInterruptibly();
        try {
            while (!this.shutdown) {
                final FetchRequest first = this.queue.peek();
                if (first == null) {
                    this.available.await(3000, TimeUnit.MILLISECONDS);
                }
                else {
                    final long delay = first.getDelay(TimeUnit.NANOSECONDS);
                    if (delay > 0) {
                        final long tl = this.available.awaitNanos(delay);
                    }
                    else {
                        final FetchRequest x = this.queue.poll();
                        assert x != null;
                        if (this.queue.size() != 0) {
                            this.available.signalAll(); // wake up other takers
                        }
                        return x;

                    }
                }
            }
        }
        finally {
            this.lock.unlock();
        }

        return null;
    }


    public void offer(final FetchRequest request) {
        this.lock.lock();
        try {
            final FetchRequest first = this.queue.peek();
            this.queue.offer(request);
            Collections.sort(this.queue);
            if (first == null || request.compareTo(first) < 0) {
                this.available.signalAll();
            }
        }
        finally {
            this.lock.unlock();
        }
    }


    public int size() {
        this.lock.lock();
        try {
            return this.queue.size();
        }
        finally {
            this.lock.unlock();
        }
    }

}
