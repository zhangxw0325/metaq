package com.taobao.metamorphosis.server.assembly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gecko.service.RemotingClient;
import com.taobao.gecko.service.RemotingFactory;
import com.taobao.gecko.service.config.ClientConfig;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.server.utils.TopicConfig;
import com.taobao.metamorphosis.utils.ZkUtils;


public class MetaMorphosisBrokerUnitTest {
    private MetaMorphosisBroker broker;
    MetaConfig metaConfig;


    @Before
    public void setUp() {
        this.metaConfig = new MetaConfig();
        this.metaConfig.setDiamondZKDataId("metamorphosis.testZkConfig");
        final List<String> topics = new ArrayList<String>();
        topics.add("topic1");
        topics.add("topic2");
        final TopicConfig topicConfig = new TopicConfig("topic2", this.metaConfig);
        topicConfig.setNumPartitions(5);
        this.metaConfig.getTopicConfigMap().put("topic2", topicConfig);
        this.metaConfig.setTopics(topics);
        this.metaConfig.setBrokerId(77);
        this.metaConfig.setHostName("localhost");
        this.metaConfig.setServerPort(8199);
        this.broker = new MetaMorphosisBroker(this.metaConfig);
    }


    @Test
    public void testStartStop() throws Exception {
        this.broker.start();
        // start twice,no problem
        this.broker.start();

        // 首先确认zk设置正确
        final BrokerZooKeeper brokerZooKeeper = this.broker.getBrokerZooKeeper();
        final ZkClient client = brokerZooKeeper.getZkClient();
        assertTrue(ZkUtils.pathExists(client, "/meta/brokers/ids/" + this.metaConfig.getBrokerId() + "/master"));
        assertEquals("meta://localhost:8199",
            ZkUtils.readData(client, "/meta/brokers/ids/" + this.metaConfig.getBrokerId() + "/master"));
        assertTrue(ZkUtils.pathExists(client, "/meta/brokers/topics/topic1/" + this.metaConfig.getBrokerId() + "-m"));
        assertTrue(ZkUtils.pathExists(client, "/meta/brokers/topics/topic2/" + this.metaConfig.getBrokerId() + "-m"));
        assertEquals("5",
            ZkUtils.readData(client, "/meta/brokers/topics/topic2/" + this.metaConfig.getBrokerId() + "-m"));

        // 兼容老客户端,临时
        assertEquals("meta://localhost:8199",
            ZkUtils.readData(client, "/meta/brokers/ids/" + this.metaConfig.getBrokerId()));
        assertEquals("5", ZkUtils.readData(client, "/meta/brokers/topics/topic2/" + this.metaConfig.getBrokerId()));

        final String serverUrl =
                ZkUtils.readData(client, "/meta/brokers/ids/" + this.metaConfig.getBrokerId() + "/master");
        assertEquals("meta://" + this.metaConfig.getHostName() + ":" + this.metaConfig.getServerPort(), serverUrl);
        assertEquals("1",
            ZkUtils.readData(client, "/meta/brokers/topics/topic1/" + this.metaConfig.getBrokerId() + "-m"));
        assertEquals("5",
            ZkUtils.readData(client, "/meta/brokers/topics/topic2/" + this.metaConfig.getBrokerId() + "-m"));

        // 确认服务器能连接
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setWireFormatType(new MetamorphosisWireFormatType());
        final RemotingClient remotingClient = RemotingFactory.connect(clientConfig);
        remotingClient.connect(serverUrl);
        remotingClient.awaitReadyInterrupt(serverUrl);
        assertTrue(remotingClient.isConnected(serverUrl));
        remotingClient.stop();

        this.broker.stop();
        // stop twice,no problem
        this.broker.stop();
    }


    @Test
    public void testStartStop_slave() throws Exception {
        this.metaConfig.setSlaveId(0);
        this.broker = new MetaMorphosisBroker(this.metaConfig);
        this.broker.start();
        // start twice,no problem
        this.broker.start();

        // 首先确认zk设置正确
        final BrokerZooKeeper brokerZooKeeper = this.broker.getBrokerZooKeeper();
        final ZkClient client = brokerZooKeeper.getZkClient();
        assertTrue(ZkUtils.pathExists(client, "/meta/brokers/ids/" + this.metaConfig.getBrokerId() + "/slave0"));
        assertTrue(ZkUtils.pathExists(client, "/meta/brokers/topics/topic1/" + this.metaConfig.getBrokerId() + "-s0"));
        assertTrue(ZkUtils.pathExists(client, "/meta/brokers/topics/topic2/" + this.metaConfig.getBrokerId() + "-s0"));
        final String serverUrl =
                ZkUtils.readData(client, "/meta/brokers/ids/" + this.metaConfig.getBrokerId() + "/slave0");
        assertEquals("meta://" + this.metaConfig.getHostName() + ":" + this.metaConfig.getServerPort(), serverUrl);
        assertEquals("1",
            ZkUtils.readData(client, "/meta/brokers/topics/topic1/" + this.metaConfig.getBrokerId() + "-s0"));
        assertEquals("5",
            ZkUtils.readData(client, "/meta/brokers/topics/topic2/" + this.metaConfig.getBrokerId() + "-s0"));

        // 确认服务器能连接
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setWireFormatType(new MetamorphosisWireFormatType());
        final RemotingClient remotingClient = RemotingFactory.connect(clientConfig);
        remotingClient.connect(serverUrl);
        remotingClient.awaitReadyInterrupt(serverUrl);
        assertTrue(remotingClient.isConnected(serverUrl));
        remotingClient.stop();

        this.broker.stop();
        // stop twice,no problem
        this.broker.stop();
    }


    @After
    public void tearDown() {
        if (this.broker != null) {
            this.broker.stop();
        }
    }
}
