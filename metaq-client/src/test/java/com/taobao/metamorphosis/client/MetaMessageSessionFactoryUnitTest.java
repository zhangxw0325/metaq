package com.taobao.metamorphosis.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.RoundRobinPartitionSelector;
import com.taobao.metamorphosis.exception.InvalidConsumerConfigException;


public class MetaMessageSessionFactoryUnitTest {
    private MetaMessageSessionFactory messageSessionFactory;


    @Before
    public void setUp() throws Exception {
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setDiamondZKDataId("metamorphosis.testZkConfig");
        this.messageSessionFactory = new MetaMessageSessionFactory(metaClientConfig);
    }


    @After
    public void tearDown() throws Exception {
        this.messageSessionFactory.shutdown();
    }


    @Test
    public void testCreateProducer() throws Exception {
        final MessageProducer producer = this.messageSessionFactory.createProducer();
        assertNotNull(producer);
        assertTrue(producer.getPartitionSelector() instanceof RoundRobinPartitionSelector);
        assertFalse(producer.isOrdered());
        assertTrue(this.messageSessionFactory.getChildren().contains(producer));
        producer.shutdown();
        assertFalse(this.messageSessionFactory.getChildren().contains(producer));
    }


    @Ignore
    public void testCreateProducerOrdered() throws Exception {
        final MessageProducer producer = this.messageSessionFactory.createProducer(true);
        assertNotNull(producer);
        assertTrue(producer.getPartitionSelector() instanceof RoundRobinPartitionSelector);
        assertTrue(producer.isOrdered());
        assertTrue(this.messageSessionFactory.getChildren().contains(producer));
        producer.shutdown();
        assertFalse(this.messageSessionFactory.getChildren().contains(producer));
    }


    @Test(expected = InvalidConsumerConfigException.class)
    public void testCreateConsumer_NoGroup() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        final MessageConsumer messageConsumer = this.messageSessionFactory.createConsumer(consumerConfig);
    }


    @Test(expected = InvalidConsumerConfigException.class)
    public void testCreateConsumer_InvalidThreadCount() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setGroup("test");
        consumerConfig.setFetchRunnerCount(0);
        final MessageConsumer messageConsumer = this.messageSessionFactory.createConsumer(consumerConfig);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testCreateConsumer_InvalidCommitOffsetsInterval() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setGroup("test");
        consumerConfig.setCommitOffsetPeriodInMills(-1);
        final MessageConsumer messageConsumer = this.messageSessionFactory.createConsumer(consumerConfig);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testCreateConsumer_InvalidFetchTimeout() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setGroup("test");
        consumerConfig.setFetchTimeoutInMills(0);
        final MessageConsumer messageConsumer = this.messageSessionFactory.createConsumer(consumerConfig);
    }


    @Test
    public void testCreateConsumer() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setGroup("test");
        final MessageConsumer messageConsumer = this.messageSessionFactory.createConsumer(consumerConfig);
        assertNotNull(messageConsumer);
        assertTrue(this.messageSessionFactory.getChildren().contains(messageConsumer));
        messageConsumer.shutdown();
        assertFalse(this.messageSessionFactory.getChildren().contains(messageConsumer));
    }

}
