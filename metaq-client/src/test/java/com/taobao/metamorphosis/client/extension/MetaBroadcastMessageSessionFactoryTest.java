package com.taobao.metamorphosis.client.extension;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-13 ÏÂÎç06:41:33
 */

public class MetaBroadcastMessageSessionFactoryTest {

    @Test
    public void testUpdateGroupForBroadcast() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig("test");
        MetaBroadcastMessageSessionFactory.updateGroupForBroadcast(consumerConfig);
        assertFalse("test".equals(consumerConfig.getGroup()));
        assertTrue(consumerConfig.getGroup().startsWith("test"));
        assertFalse(consumerConfig.getGroup().contains("."));
        System.out.println(consumerConfig.getGroup());
    }
}
