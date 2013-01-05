/**
 * $Id: TestVolatile.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.research;

class SimpleThreadPool {
    private final Thread[] threads;


    public SimpleThreadPool(final int poolSize, final Runnable task) {
        assert poolSize > 0;
        threads = new Thread[poolSize];
        for (int i = 0; i < poolSize; i++) {
            threads[i] = new Thread(task);
        }
    }


    public void start() {
        for (Thread thread : this.threads) {
            thread.start();
        }
    }


    public void stop() {
    }
}


class ShareVar {
    private long x;
    private long y;


    public ShareVar(long x, long y) {
        this.x = x;
        this.y = y;
    }


    @Override
    public String toString() {
        return String.valueOf(x) + "-" + String.valueOf(y);
    }
}


public class TestVolatile {
    private ShareVar shareVar = new ShareVar(1, 1);


    public ShareVar getShareVar() {
        return shareVar;
    }


    public void setShareVar(ShareVar shareVar) {
        this.shareVar = shareVar;
    }


    public void print() {
        System.out.println("in read thread shareVar = " + TestVolatile.this.shareVar);
    }


    public static void main(String[] args) {
        final TestVolatile test = new TestVolatile();

        SimpleThreadPool readThreads = new SimpleThreadPool(100, new Runnable() {
            public void run() {
                while (true) {
                    test.print();
                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        SimpleThreadPool writeThreads = new SimpleThreadPool(10, new Runnable() {

            public void run() {
                while (true) {
                    ShareVar shareVar = new ShareVar(System.currentTimeMillis(), System.currentTimeMillis());
                    test.setShareVar(shareVar);

                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        readThreads.start();

        try {
            Thread.sleep(1000 * 60);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        writeThreads.start();
    }
}
