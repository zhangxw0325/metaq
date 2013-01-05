package com.taobao.metamorphosis.utils;

import static org.junit.Assert.assertFalse;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.utils.test.ConcurrentTestCase;
import com.taobao.metamorphosis.utils.test.ConcurrentTestTask;


public class IdWorkerUnitTest {
    private IdWorker idWorker1, idWorker2;


    @Before
    public void setUp() {
        this.idWorker1 = new IdWorker(0);
        this.idWorker2 = new IdWorker(1);
    }


    @Test
    public void testNextIdThree() {
        final long id1 = this.idWorker1.nextId();
        final long id2 = this.idWorker1.nextId();
        final long id3 = this.idWorker1.nextId();

        assertFalse(id1 == id2);
        assertFalse(id1 == id3);
        assertFalse(id2 == id3);
    }


    @Test
    public void concurrentTest() {
        final AtomicLong ids = new AtomicLong();
        final ConcurrentTestCase testCase = new ConcurrentTestCase(100, 10000, new ConcurrentTestTask() {

            @Override
            public void run(final int index, final int times) throws Exception {
                final long id = IdWorkerUnitTest.this.idWorker1.nextId();
                if (id < 0) {
                    throw new RuntimeException("error");
                }
                ids.addAndGet(id);
            }
        });
        testCase.start();
        System.out.println(ids.get());
        System.out.println(testCase.getDurationInMillis());
    }


    @Test
    public void testNextIdTwoWorkers() {
        final long id1 = this.idWorker1.nextId();
        final long id2 = this.idWorker2.nextId();
        final long id3 = this.idWorker1.nextId();
        final long id4 = this.idWorker2.nextId();

        assertFalse(id1 == id2);
        assertFalse(id1 == id3);
        assertFalse(id2 == id3);
        assertFalse(id1 == id4);
        assertFalse(id2 == id4);
        assertFalse(id3 == id4);
        assertFalse(id2 == id4);
    }
}
