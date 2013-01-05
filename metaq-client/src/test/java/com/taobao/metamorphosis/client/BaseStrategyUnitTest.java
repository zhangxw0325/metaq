package com.taobao.metamorphosis.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;


public abstract class BaseStrategyUnitTest {
    public List<String> createPartitions(final String topic, final int num) {
        final List<String> rt = new ArrayList<String>();
        for (int i = 0; i < num; i++) {
            rt.add(topic + "-" + i);
        }
        return rt;
    }


    public void assertInclude(final List<String> parts, final String... expects) {
        System.out.println(parts);
        for (final String expect : expects) {
            assertTrue(parts.contains(expect));
        }
        assertEquals(parts.size(), expects.length);
    }


    public List<String> createConsumers(final int num) throws Exception {
        final List<String> rt = new ArrayList<String>();
        for (int i = 0; i < num; i++) {
            rt.add("consumer-" + i);
        }
        return rt;
    }
}
