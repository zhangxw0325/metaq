package com.taobao.metamorphosis.client.extension.producer;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-12-29 ÏÂÎç5:18:13
 */

public class SlidingWindowUnitTest {

    @Before
    public void setUp() throws Exception {
    }


    @Test
    public void testTryAcquireByLengthInt() {
        SlidingWindow window = new SlidingWindow(3);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2));

        window.releaseByLenth(4096 * 2);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2));
    }


    @Test
    public void testTryAcquireByLengthIntLongTimeUnit() throws InterruptedException {
        SlidingWindow window = new SlidingWindow(3);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));
        Assert.assertFalse(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));

        window.releaseByLenth(4096 * 2);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2, 500, TimeUnit.MILLISECONDS));
    }


    @Test
    public void testReleaseByLenth() {

    }


    @Test
    public void testGetWindowsSize() {
        SlidingWindow window = new SlidingWindow(3);
        Assert.assertTrue(window.tryAcquireByLength(4096 * 2));
        Assert.assertTrue(window.getWindowsSize() == 3);
    }

}
