package com.taobao.metamorphosis.client.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.producer.ProducerZooKeeper.BrokerConnectionListener;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.DiamondUtils;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class ProducerZooKeeperUnitTest {
    private ProducerZooKeeper producerZooKeeper;
    private ZkClient client;
    private DiamondManager diamondManager;
    private ZKConfig zkConfig;
    private IMocksControl mocksControl;
    private RemotingClientWrapper remotingClient;
    private MetaZookeeper metaZookeeper;


    @Before
    public void setUp() throws Exception {
        this.mocksControl = EasyMock.createControl();
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.diamondManager = new DefaultDiamondManager(null, "metamorphosis.testZkConfig", (ManagerListener) null);
        this.zkConfig = DiamondUtils.getZkConfig(this.diamondManager, 10000);
        this.client =
                new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
        this.metaZookeeper = new MetaZookeeper(this.client, this.zkConfig.zkRoot);
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setDiamondZKDataId("metamorphosis.testZkConfig");
        this.producerZooKeeper =
                new ProducerZooKeeper(this.metaZookeeper, this.remotingClient, this.client, metaClientConfig);
    }


    @Test
    public void testSetDefaultTopic() throws Exception {
        final String topic = "topic1";
        final String defaultTopic = "topic2";
        this.testPublishTopic(defaultTopic);
        this.producerZooKeeper.setDefaultTopic(defaultTopic);
        final PartitionSelector selector = new RoundRobinPartitionSelector();
        assertEquals(new Partition("1-0"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals(new Partition("1-1"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals(new Partition("0-0"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals(new Partition("1-0"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals("meta://localhost:0", this.producerZooKeeper.selectBroker(topic, new Partition("0-0")));
        assertEquals("meta://localhost:1", this.producerZooKeeper.selectBroker(topic, new Partition("1-0")));
        assertEquals("meta://localhost:1", this.producerZooKeeper.selectBroker(topic, new Partition("1-1")));
    }


    @Test
    public void testPublishTopic_AddBroker_DelBroker() throws Exception {
        final String topic = "topic1";

        final BrokerConnectionListener listener = this.testPublishTopic(topic);
        this.mocksControl.reset();
        this.testAddGroup(topic, listener);
        this.mocksControl.reset();
        // 删除一个topic
        this.testRemoveBroker(topic, listener);

    }


    @Test
    public void testPublishTopic_AddBroker_DelBroker_withSlave() throws Exception {
        final String topic = "topic1";

        // 注册slave broker和topic信息
        // 对生产者不影响
        for (int i = 0; i < 2; i++) {
            final String brokerIdPath = this.metaZookeeper.brokerIdsPath + "/" + i + "/slave0";
            final String brokerTopicPath = this.metaZookeeper.brokerTopicsPath + "/" + topic + "/" + i + "-s0";
            ZkUtils.createEphemeralPath(this.client, brokerIdPath, "meta://localhost:" + i);
            ZkUtils.createEphemeralPath(this.client, brokerTopicPath, String.valueOf(i + 1));
        }

        final BrokerConnectionListener listener = this.testPublishTopic(topic);
        this.mocksControl.reset();
        this.testAddGroup(topic, listener);
        this.mocksControl.reset();
        // 删除一个topic
        this.testRemoveBroker(topic, listener);

    }


    private void testRemoveBroker(final String topic, final BrokerConnectionListener listener)
            throws NotifyRemotingException, Exception, InterruptedException {
        this.remotingClient.close("meta://localhost:0", false);
        EasyMock.expectLastCall();

        this.mocksControl.replay();
        // 关闭broker 0
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerIdsPath + "/" + 0 + "/master");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsPath + "/" + topic + "/" + 0 + "-m");
        Thread.sleep(1000);
        final Map<Integer, String> brokerMap = listener.brokersInfo.oldBrokerStringMap;
        assertEquals(2, brokerMap.size());
        assertEquals("meta://localhost:1", brokerMap.get(1));
        assertEquals("meta://localhost:2", brokerMap.get(2));

        final Map<String, List<Partition>> topicPartitionMap = listener.brokersInfo.oldTopicPartitionMap;
        assertEquals(1, topicPartitionMap.size());
        final List<Partition> partList = topicPartitionMap.get(topic);
        assertFalse(partList.contains(new Partition("0-0")));
        assertTrue(partList.contains(new Partition("1-0")));
        assertTrue(partList.contains(new Partition("1-1")));
        assertTrue(partList.contains(new Partition("2-0")));
        assertTrue(partList.contains(new Partition("2-1")));
        assertTrue(partList.contains(new Partition("2-2")));
        this.mocksControl.verify();
        this.mocksControl.verify();
    }


    private BrokerConnectionListener testPublishTopic(final String topic) throws Exception {
        // 注册broker和topic信息
        for (int i = 0; i < 2; i++) {
            final String brokerIdPath = this.metaZookeeper.brokerIdsPath + "/" + i + "/master";
            final String brokerTopicPath = this.metaZookeeper.brokerTopicsPath + "/" + topic + "/" + i + "-m";
            ZkUtils.createEphemeralPath(this.client, brokerIdPath, "meta://localhost:" + i);
            ZkUtils.createEphemeralPath(this.client, brokerTopicPath, String.valueOf(i + 1));
        }
        this.mockConnect("meta://localhost:0");
        this.mockConnect("meta://localhost:1");
        this.mocksControl.replay();
        // 发布topic
        this.producerZooKeeper.publishTopic(topic);
        this.mocksControl.verify();
        final BrokerConnectionListener listener = this.producerZooKeeper.getBrokerConnectionListener(topic);
        assertNotNull(listener);
        final Map<Integer/* broker id */, String/* server url */> brokerMap = listener.brokersInfo.oldBrokerStringMap;
        assertEquals(2, brokerMap.size());
        assertEquals("meta://localhost:0", brokerMap.get(0));
        assertEquals("meta://localhost:1", brokerMap.get(1));

        final Map<String/* topic */, List<Partition>/* partition list */> topicPartitionMap =
                listener.brokersInfo.oldTopicPartitionMap;
        assertEquals(1, topicPartitionMap.size());
        final List<Partition> partList = topicPartitionMap.get(topic);
        assertTrue(partList.contains(new Partition("0-0")));
        assertTrue(partList.contains(new Partition("1-0")));
        assertTrue(partList.contains(new Partition("1-1")));
        return listener;
    }


    private void testAddGroup(final String topic, final BrokerConnectionListener listener)
            throws NotifyRemotingException, InterruptedException, Exception {
        Map<Integer, String> brokerMap;
        Map<String, List<Partition>> topicPartitionMap;
        List<Partition> partList;
        this.mockConnect("meta://localhost:2");
        this.mocksControl.replay();
        // 发布一个新的broker
        final String brokerIdPath = this.metaZookeeper.brokerIdsPath + "/" + 2 + "/master";
        final String brokerTopicPath = this.metaZookeeper.brokerTopicsPath + "/" + topic + "/" + 2 + "-m";
        ZkUtils.createEphemeralPath(this.client, brokerIdPath, "meta://localhost:" + 2);
        ZkUtils.createEphemeralPath(this.client, brokerTopicPath, String.valueOf(3));

        Thread.sleep(1000);
        brokerMap = listener.brokersInfo.oldBrokerStringMap;
        assertEquals(3, brokerMap.size());
        assertEquals("meta://localhost:0", brokerMap.get(0));
        assertEquals("meta://localhost:1", brokerMap.get(1));
        assertEquals("meta://localhost:2", brokerMap.get(2));

        topicPartitionMap = listener.brokersInfo.oldTopicPartitionMap;
        assertEquals(1, topicPartitionMap.size());
        partList = topicPartitionMap.get(topic);
        assertTrue(partList.contains(new Partition("0-0")));
        assertTrue(partList.contains(new Partition("1-0")));
        assertTrue(partList.contains(new Partition("1-1")));
        assertTrue(partList.contains(new Partition("2-0")));
        assertTrue(partList.contains(new Partition("2-1")));
        assertTrue(partList.contains(new Partition("2-2")));
        this.mocksControl.verify();
    }


    @Test
    public void testSelectPartitionSelectBroker() throws Exception {
        final String topic = "topic2";
        this.testPublishTopic(topic);
        final PartitionSelector selector = new RoundRobinPartitionSelector();
        assertEquals(new Partition("1-0"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals(new Partition("1-1"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals(new Partition("0-0"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals(new Partition("1-0"), this.producerZooKeeper.selectPartition(topic, null, selector));
        assertEquals("meta://localhost:0", this.producerZooKeeper.selectBroker(topic, new Partition("0-0")));
        assertEquals("meta://localhost:1", this.producerZooKeeper.selectBroker(topic, new Partition("1-0")));
        assertEquals("meta://localhost:1", this.producerZooKeeper.selectBroker(topic, new Partition("1-1")));
    }


    private void mockConnect(final String url) throws NotifyRemotingException, InterruptedException {
        this.remotingClient.connect(url);
        EasyMock.expectLastCall();
        this.remotingClient.awaitReadyInterrupt(url);
        EasyMock.expectLastCall();
    }


    @After
    public void tearDown() {
        this.client.close();
    }
}
