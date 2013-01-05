package com.taobao.metamorphosis.client.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.ConsumerZooKeeper.ZKLoadRebalanceListener;
import com.taobao.metamorphosis.client.consumer.storage.OffsetStorage;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.DiamondUtils;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.MetaZookeeper.ZKGroupDirs;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class ConsumerZooKeeperUnitTest {
    private ConsumerZooKeeper consumerZooKeeper;
    private ZkClient client;
    private DiamondManager diamondManager;
    private ZKConfig zkConfig;
    private RemotingClientWrapper remotingClient;
    private FetchManager fetchManager;
    private OffsetStorage offsetStorage;
    private LoadBalanceStrategy loadBalanceStrategy;
    private IMocksControl mocksControl;
    private MetaZookeeper metaZookeeper;

    private static final long START_OFFSET = 0;// Long.MAX_VALUE;


    @Before
    public void setUp() {
        this.mocksControl = EasyMock.createControl();
        this.offsetStorage = this.mocksControl.createMock(OffsetStorage.class);
        this.remotingClient = this.mocksControl.createMock(RemotingClientWrapper.class);
        this.fetchManager = this.mocksControl.createMock(FetchManager.class);
        this.loadBalanceStrategy = new DefaultLoadBalanceStrategy();
        this.diamondManager = new DefaultDiamondManager(null, "metamorphosis.testZkConfig", (ManagerListener) null);
        this.zkConfig = DiamondUtils.getZkConfig(this.diamondManager, 10000);

        this.client =
                new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
        this.metaZookeeper = new MetaZookeeper(this.client, this.zkConfig.zkRoot);
        this.consumerZooKeeper =
                new ConsumerZooKeeper(this.metaZookeeper, this.remotingClient, this.client, this.zkConfig);
    }


    @Test
    public void testReigsterConsumerOneConsumer() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        final String group = "meta-test";
        consumerConfig.setGroup(group);
        final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry =
                new ConcurrentHashMap<String, SubscriberInfo>();
        topicSubcriberRegistry.put("topic1", new SubscriberInfo(null, 1024 * 1024, null));
        topicSubcriberRegistry.put("topic2", new SubscriberInfo(null, 1024 * 1024, null));

        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsPath + "/topic1/0-m");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsPath + "/topic2/0-m");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsPath + "/topic2/1-m");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsPath + "/topic2/2-m");
        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/0/master", "meta://localhost:0");
        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/1/master", "meta://localhost:1");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsPath + "/topic1/0-m", "3");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsPath + "/topic2/0-m", "1");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsPath + "/topic2/1-m", "1");

        this.mockConnect("meta://localhost:0");
        this.mockConnect("meta://localhost:1");
        this.mockCommitOffsets(group, new ArrayList<TopicPartitionRegInfo>());

        this.mockLoadNullInitOffset("topic1", group, new Partition("0-0"));
        this.mockLoadNullInitOffset("topic1", group, new Partition("0-1"));
        this.mockLoadNullInitOffset("topic1", group, new Partition("0-2"));
        this.mockLoadNullInitOffset("topic2", group, new Partition("0-0"));
        this.mockLoadNullInitOffset("topic2", group, new Partition("1-0"));

        this.mockFetchManagerRestart();
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-1"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-2"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost:1"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-0"), START_OFFSET), 1024 * 1024));

        this.mocksControl.replay();
        this.consumerZooKeeper.registerConsumer(consumerConfig, this.fetchManager, topicSubcriberRegistry,
            this.offsetStorage, this.loadBalanceStrategy);
        this.mocksControl.verify();

        // 验证订阅者分配,因为只有一个订阅者，挂载到所有分区下
        final ZKLoadRebalanceListener listener = this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager);
        assertNotNull(listener);

        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                listener.topicRegistry;
        assertNotNull(topicRegistry);
        assertFalse(topicRegistry.isEmpty());
        assertEquals(2, topicRegistry.size());

        assertTrue(topicRegistry.containsKey("topic1"));
        assertTrue(topicRegistry.containsKey("topic2"));
        this.checkTopic1(topicRegistry);
        this.checkTopic2(topicRegistry);

        final Set<Broker> brokerSet = listener.oldBrokerSet;
        assertEquals(2, brokerSet.size());
        assertTrue(brokerSet.contains(new Broker(0, "meta://localhost:0")));
        assertTrue(brokerSet.contains(new Broker(1, "meta://localhost:1")));
    }


    private void checkTopic2(
            final ConcurrentHashMap<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry) {
        final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partMap2 = topicRegistry.get("topic2");
        assertEquals(2, partMap2.size());
        assertTrue(partMap2.containsKey(new Partition("0-0")));
        assertTrue(partMap2.containsKey(new Partition("1-0")));
    }


    private void checkTopic1(
            final ConcurrentHashMap<String, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry) {
        final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partMap1 = topicRegistry.get("topic1");
        assertEquals(3, partMap1.size());
        assertTrue(partMap1.containsKey(new Partition("0-0")));
        assertTrue(partMap1.containsKey(new Partition("0-1")));
        assertTrue(partMap1.containsKey(new Partition("0-2")));
    }


    @Test
    public void testAddPartiton() throws Exception {
        // topic2增加一个分区,订阅者重新挂载到topic2下的所有分区
        this.testReigsterConsumerOneConsumer();
        Thread.sleep(1000);
        this.mocksControl.reset();
        final String group = "meta-test";
        this.mockCommitOffsets(group, this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager)
            .getTopicPartitionRegInfos());
        this.mockConnect("meta://localhost:2");
        // 0-0和1-0不需要重新加载
        // this.mockLoadNullInitOffset("topic2", group, new Partition("0-0"));
        // this.mockLoadNullInitOffset("topic2", group, new Partition("1-0"));
        this.mockLoadNullInitOffset("topic2", group, new Partition("2-0"));

        this.mockFetchManagerRestart();
        // 所有任务重新启动
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-1"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-2"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost:1"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(2, "meta://localhost:2"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("2-0"), START_OFFSET), 1024 * 1024));

        this.mocksControl.replay();

        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/2/master", "meta://localhost:2");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsPath + "/topic2/2-m", "1");
        Thread.sleep(5000);
        this.mocksControl.verify();
        final ZKLoadRebalanceListener listener = this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager);
        assertNotNull(listener);

        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                listener.topicRegistry;
        assertNotNull(topicRegistry);
        assertFalse(topicRegistry.isEmpty());
        assertEquals(2, topicRegistry.size());

        assertTrue(topicRegistry.containsKey("topic1"));
        assertTrue(topicRegistry.containsKey("topic2"));
        this.checkTopic1(topicRegistry);
        final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partMap2 = topicRegistry.get("topic2");
        assertEquals(3, partMap2.size());
        assertTrue(partMap2.containsKey(new Partition("0-0")));
        assertTrue(partMap2.containsKey(new Partition("1-0")));
        assertTrue(partMap2.containsKey(new Partition("2-0")));

        final Set<Broker> brokerSet = listener.oldBrokerSet;
        assertEquals(3, brokerSet.size());
        assertTrue(brokerSet.contains(new Broker(0, "meta://localhost:0")));
        assertTrue(brokerSet.contains(new Broker(1, "meta://localhost:1")));
        assertTrue(brokerSet.contains(new Broker(2, "meta://localhost:2")));
    }


    @Test
    public void testAddRemoveConsumer() throws Exception {
        this.testReigsterConsumerOneConsumer();
        // 添加同一分组的订阅者订阅topic1，现在本consumer只挂载到2个分区，另一个分区分配给新的订阅者
        Thread.sleep(1000);
        this.testAddConsumer();
        this.testRemove();

    }


    @Test
    public void testReigsterConsumer_MasterAndSlaveStarted() throws Exception {
        final ConsumerConfig consumerConfig = new ConsumerConfig();
        final String group = "meta-test";
        consumerConfig.setGroup(group);
        final ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubcriberRegistry =
                new ConcurrentHashMap<String, SubscriberInfo>();
        topicSubcriberRegistry.put("topic1", new SubscriberInfo(null, 1024 * 1024, null));
        topicSubcriberRegistry.put("topic2", new SubscriberInfo(null, 1024 * 1024, null));

        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/0/master", "meta://localhost:0");
        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/0/slave0", "meta://localhost1:0");
        ZkUtils.createEphemeralPath(this.client, this.metaZookeeper.brokerIdsPath + "/1/master", "meta://localhost2:0");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsPath + "/topic1/0-m", "2");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsPath + "/topic1/0-s0", "2");
        this.client.createEphemeral(this.metaZookeeper.brokerTopicsPath + "/topic2/1-m", "2");

        this.mockConnectAnyTimes("meta://localhost:0");
        this.mockConnectAnyTimes("meta://localhost1:0");
        this.mockConnect("meta://localhost2:0");
        this.mockCommitOffsets(group, new ArrayList<TopicPartitionRegInfo>());

        this.mockLoadNullInitOffset("topic1", group, new Partition("0-0"));
        this.mockLoadNullInitOffset("topic1", group, new Partition("0-1"));
        this.mockLoadNullInitOffset("topic2", group, new Partition("1-0"));
        this.mockLoadNullInitOffset("topic2", group, new Partition("1-1"));

        this.mockFetchManagerRestart();

        // 选取主备是随机的
        this.fetchManager.addFetchRequest((FetchRequest) org.easymock.EasyMock.anyObject());
        org.easymock.EasyMock.expectLastCall();
        this.fetchManager.addFetchRequest((FetchRequest) org.easymock.EasyMock.anyObject());
        org.easymock.EasyMock.expectLastCall();

        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost2:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost2:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-1"), START_OFFSET), 1024 * 1024));

        this.mocksControl.replay();
        this.consumerZooKeeper.registerConsumer(consumerConfig, this.fetchManager, topicSubcriberRegistry,
            this.offsetStorage, this.loadBalanceStrategy);
        this.mocksControl.verify();

        final ZKLoadRebalanceListener listener = this.checkTopicMasterSlave();

        final Set<Broker> brokerSet = listener.oldBrokerSet;
        for (final Broker broker : brokerSet) {
            System.out.println(broker);
        }
        assertTrue(brokerSet.size() >= 2 && brokerSet.size() <= 3);
        assertTrue(brokerSet.contains(new Broker(1, "meta://localhost2:0")));
    }


    @Test
    public void testReigsterConsumer_MasterAndSlaveStarted_thenMasterDown() throws Exception {
        this.testReigsterConsumer_MasterAndSlaveStarted();

        this.mocksControl.reset();
        this.mockConnectAnyTimes("meta://localhost1:0");

        this.mockFetchManagerRestart();

        // 选取备的
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost1:0?slaveId=0"), 0,
            new TopicPartitionRegInfo("topic1", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost1:0?slaveId=0"), 0,
            new TopicPartitionRegInfo("topic1", new Partition("0-1"), START_OFFSET), 1024 * 1024));

        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost2:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost2:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-1"), START_OFFSET), 1024 * 1024));

        this.mockConnectCloseAnyTimes("meta://localhost:0");

        this.mocksControl.replay();
        // --------假设master这时候挂掉---
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerIdsPath + "/0/master");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsPath + "/topic1/0-m");

        // 等待重新负载均衡结束
        Thread.sleep(5000);

        this.mocksControl.verify();

        // 验证重新订阅者分配,连接到meta://localhost1:0和meta://localhost2:0
        final ZKLoadRebalanceListener listener = this.checkTopicMasterSlave();

        final Set<Broker> brokerSet = listener.oldBrokerSet;
        for (final Broker broker : brokerSet) {
            System.out.println(broker);
        }
        assertTrue(brokerSet.size() == 2);
        assertTrue(brokerSet.contains(new Broker(0, "meta://localhost1:0?slaveId=0")));
        assertTrue(brokerSet.contains(new Broker(1, "meta://localhost2:0")));
    }


    @Test
    public void testReigsterConsumer_MasterAndSlaveStarted_thenSlaveDown() throws Exception {
        this.testReigsterConsumer_MasterAndSlaveStarted();

        this.mocksControl.reset();
        this.mockConnectAnyTimes("meta://localhost:0");

        this.mockFetchManagerRestart();

        // 选取主的
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-1"), START_OFFSET), 1024 * 1024));

        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost2:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost2:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-1"), START_OFFSET), 1024 * 1024));

        this.mockConnectCloseAnyTimes("meta://localhost1:0");

        this.mocksControl.replay();
        // --------假设slave这时候挂掉---
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerIdsPath + "/0/slave0");
        ZkUtils.deletePath(this.client, this.metaZookeeper.brokerTopicsPath + "/topic1/0-s0");

        // 等待重新负载均衡结束
        Thread.sleep(5000);

        this.mocksControl.verify();

        // 验证重新订阅者分配,连接到meta://localhost:0和meta://localhost2:0
        final ZKLoadRebalanceListener listener = this.checkTopicMasterSlave();

        final Set<Broker> brokerSet = listener.oldBrokerSet;
        for (final Broker broker : brokerSet) {
            System.out.println(broker);
        }
        assertTrue(brokerSet.size() == 2);
        assertTrue(brokerSet.contains(new Broker(0, "meta://localhost:0")));
        assertTrue(brokerSet.contains(new Broker(1, "meta://localhost2:0")));
    }


    private ZKLoadRebalanceListener checkTopicMasterSlave() {
        final ZKLoadRebalanceListener listener = this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager);
        assertNotNull(listener);

        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                listener.topicRegistry;
        assertNotNull(topicRegistry);
        assertFalse(topicRegistry.isEmpty());
        assertEquals(2, topicRegistry.size());

        assertTrue(topicRegistry.containsKey("topic1"));
        assertTrue(topicRegistry.containsKey("topic2"));
        assertTrue(topicRegistry.get("topic1").containsKey(new Partition("0-0")));
        assertTrue(topicRegistry.get("topic1").containsKey(new Partition("0-1")));

        assertEquals(2, topicRegistry.get("topic2").size());
        assertTrue(topicRegistry.get("topic2").containsKey(new Partition("1-0")));
        assertTrue(topicRegistry.get("topic2").containsKey(new Partition("1-1")));
        return listener;
    }


    private void testRemove() throws NotifyRemotingException, InterruptedException, Exception {
        this.mocksControl.reset();
        final String group = "meta-test";
        this.mockCommitOffsets(group, this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager)
            .getTopicPartitionRegInfos());
        // 0-0,0-1不需要重新加载，但是0-3是新的，需要重新加载
        // this.mockLoadNullInitOffset("topic1", group, new Partition("0-0"));
        // this.mockLoadNullInitOffset("topic1", group, new Partition("0-1"));
        this.mockLoadNullInitOffset("topic1", group, new Partition("0-2"));

        this.mockFetchManagerRestart();
        // 所有任务重新启动
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-1"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-2"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost:1"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-0"), START_OFFSET), 1024 * 1024));

        this.mocksControl.replay();
        // 注册consumer id，订阅topic1
        final ZKGroupDirs dirs = this.metaZookeeper.new ZKGroupDirs(group);
        ZkUtils.deletePath(this.client, dirs.consumerRegistryDir + "/new-consumer");
        Thread.sleep(5000);
        this.mocksControl.verify();

        final ZKLoadRebalanceListener listener = this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager);
        assertNotNull(listener);

        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                listener.topicRegistry;
        assertNotNull(topicRegistry);
        assertFalse(topicRegistry.isEmpty());
        assertEquals(2, topicRegistry.size());

        assertTrue(topicRegistry.containsKey("topic1"));
        assertTrue(topicRegistry.containsKey("topic2"));

        final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partMap1 = topicRegistry.get("topic1");
        assertEquals(3, partMap1.size());
        assertTrue(partMap1.containsKey(new Partition("0-0")));
        assertTrue(partMap1.containsKey(new Partition("0-1")));
        assertTrue(partMap1.containsKey(new Partition("0-2")));

        this.checkTopic2(topicRegistry);
    }


    private void testAddConsumer() throws NotifyRemotingException, InterruptedException, Exception {
        this.mocksControl.reset();
        final String group = "meta-test";
        this.mockCommitOffsets(group, this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager)
            .getTopicPartitionRegInfos());
        // 0-0,0-1不需要重新加载
        // this.mockLoadNullInitOffset("topic1", group, new Partition("0-0"));
        // this.mockLoadNullInitOffset("topic1", group, new Partition("0-1"));

        this.mockFetchManagerRestart();
        // 所有任务重新启动
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic1", new Partition("0-1"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(0, "meta://localhost:0"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("0-0"), START_OFFSET), 1024 * 1024));
        this.mockAddFetchRequest(new FetchRequest(new Broker(1, "meta://localhost:1"), 0, new TopicPartitionRegInfo(
            "topic2", new Partition("1-0"), START_OFFSET), 1024 * 1024));

        this.mocksControl.replay();
        // 注册consumer id，订阅topic1
        final ZKGroupDirs dirs = this.metaZookeeper.new ZKGroupDirs(group);
        ZkUtils.createEphemeralPathExpectConflict(this.client, dirs.consumerRegistryDir + "/new-consumer", "topic1");
        Thread.sleep(5000);
        this.mocksControl.verify();

        final ZKLoadRebalanceListener listener = this.consumerZooKeeper.getBrokerConnectionListener(this.fetchManager);
        assertNotNull(listener);

        final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Partition, TopicPartitionRegInfo>> topicRegistry =
                listener.topicRegistry;
        assertNotNull(topicRegistry);
        assertFalse(topicRegistry.isEmpty());
        assertEquals(2, topicRegistry.size());

        assertTrue(topicRegistry.containsKey("topic1"));
        assertTrue(topicRegistry.containsKey("topic2"));

        final ConcurrentHashMap<Partition, TopicPartitionRegInfo> partMap1 = topicRegistry.get("topic1");
        assertEquals(2, partMap1.size());
        assertTrue(partMap1.containsKey(new Partition("0-0")));
        assertTrue(partMap1.containsKey(new Partition("0-1")));
        // assertTrue(partMap1.containsKey(new Partition("0-2")));

        this.checkTopic2(topicRegistry);
    }


    private void mockFetchManagerRestart() throws InterruptedException {
        this.fetchManager.stopFetchRunner();
        org.easymock.EasyMock.expectLastCall();
        this.fetchManager.resetFetchState();
        org.easymock.EasyMock.expectLastCall();
        this.fetchManager.startFetchRunner();
        org.easymock.EasyMock.expectLastCall();
    }


    private void mockAddFetchRequest(final FetchRequest fetchRequest) {
        this.fetchManager.addFetchRequest(fetchRequest);
        org.easymock.EasyMock.expectLastCall();
    }


    // 分组id随时产生，无法模拟，使用anyObject
    private void mockLoadNullInitOffset(final String topic, final String group, final Partition partition) {
        org.easymock.EasyMock.expect(
            this.offsetStorage.load(org.easymock.EasyMock.eq(topic), org.easymock.EasyMock.contains(group),
                org.easymock.EasyMock.eq(partition))).andReturn(null);
        this.offsetStorage.initOffset(org.easymock.EasyMock.eq(topic), org.easymock.EasyMock.contains(group),
            org.easymock.EasyMock.eq(partition), org.easymock.EasyMock.eq(START_OFFSET));
        org.easymock.EasyMock.expectLastCall();
    }


    private void mockCommitOffsets(final String group, final Collection<TopicPartitionRegInfo> arrayList) {
        this.offsetStorage.commitOffset(group, arrayList);
        org.easymock.EasyMock.expectLastCall();
    }


    private void mockConnect(final String url) throws NotifyRemotingException, InterruptedException {
        this.remotingClient.connect(url);
        org.easymock.EasyMock.expectLastCall();
        this.remotingClient.awaitReadyInterrupt(url);
        org.easymock.EasyMock.expectLastCall();
    }


    private void mockConnectAnyTimes(final String url) throws NotifyRemotingException, InterruptedException {
        this.remotingClient.connect(url);
        org.easymock.EasyMock.expectLastCall().anyTimes();
        this.remotingClient.awaitReadyInterrupt(url);
        org.easymock.EasyMock.expectLastCall().anyTimes();
    }


    private void mockConnectCloseAnyTimes(final String url) throws NotifyRemotingException, InterruptedException {
        this.remotingClient.close(url, false);
        org.easymock.EasyMock.expectLastCall().anyTimes();
    }


    @After
    public void tearDown() {
        this.diamondManager.close();
        this.client.close();
    }

}
