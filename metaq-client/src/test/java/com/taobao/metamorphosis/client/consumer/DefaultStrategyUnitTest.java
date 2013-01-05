package com.taobao.metamorphosis.client.consumer;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.taobao.metamorphosis.client.BaseStrategyUnitTest;


public class DefaultStrategyUnitTest extends BaseStrategyUnitTest {
    private final LoadBalanceStrategy strategy = new DefaultLoadBalanceStrategy();


    @Test
    public void testGetPartitions_4consumers_10partitions() throws Exception {
        final String topic = "test";
        final List<String> curConsumers = this.createConsumers(4);
        final List<String> curPartitions = this.createPartitions(topic, 10);
        this.assertInclude(this.strategy.getPartitions(topic, "consumer-0", curConsumers, curPartitions), "test-0",
            "test-1", "test-2");
        this.assertInclude(this.strategy.getPartitions(topic, "consumer-1", curConsumers, curPartitions), "test-3",
            "test-4", "test-5");
        this.assertInclude(this.strategy.getPartitions(topic, "consumer-2", curConsumers, curPartitions), "test-6",
            "test-7");
        this.assertInclude(this.strategy.getPartitions(topic, "consumer-3", curConsumers, curPartitions), "test-8",
            "test-9");

        assertTrue(this.strategy.getPartitions(topic, "consumer-100", curConsumers, curPartitions).isEmpty());
    }


    @Test
    public void testGetPartitions_3consumers_10partitions() throws Exception {
        final String topic = "test";
        final List<String> curConsumers = this.createConsumers(3);
        final List<String> curPartitions = this.createPartitions(topic, 10);
        this.assertInclude(this.strategy.getPartitions(topic, "consumer-0", curConsumers, curPartitions), "test-0",
            "test-1", "test-2", "test-3");
        this.assertInclude(this.strategy.getPartitions(topic, "consumer-1", curConsumers, curPartitions), "test-4",
            "test-5", "test-6");
        this.assertInclude(this.strategy.getPartitions(topic, "consumer-2", curConsumers, curPartitions), "test-7",
            "test-8", "test-9");

        assertTrue(this.strategy.getPartitions(topic, "consumer-100", curConsumers, curPartitions).isEmpty());
    }
}
