package com.taobao.metamorphosis.client.consumer;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-11-14 ÏÂÎç2:29:14
 */

public class ConsumerConfigUnitTest {
    private ConsumerConfig consumerConfig;
    private final String group = "test-group";


    @Before
    public void setUp() {
        this.consumerConfig = new ConsumerConfig(group);

    }


    @Test
    public void testSetConsumeFromMaxOffset() {
        assertEquals(0, this.consumerConfig.getOffset());
        this.consumerConfig.setConsumeFromMaxOffset();
        assertEquals(Long.MAX_VALUE, this.consumerConfig.getOffset());
    }

}
