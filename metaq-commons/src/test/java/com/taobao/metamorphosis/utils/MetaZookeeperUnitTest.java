package com.taobao.metamorphosis.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import com.taobao.metamorphosis.cluster.Broker;
import com.taobao.metamorphosis.cluster.Cluster;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class MetaZookeeperUnitTest {
    private MetaZookeeper metaZookeeper;
    private ZkClient client;
    private DiamondManager diamondManager;
    private ZKConfig zkConfig;


    @Before
    public void setUp() {
        this.diamondManager = new DefaultDiamondManager(null, "metamorphosis.testZkConfig", (ManagerListener) null);
        this.zkConfig = DiamondUtils.getZkConfig(this.diamondManager, 10000);
        this.client =
                new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
        this.metaZookeeper = new MetaZookeeper(this.client, "/meta");
    }


    @Test
    public void testGetPartitionStringsForTopics() throws Exception {
        final String masterTopicPath = "/meta/brokers/topics/xxtopic/0-m";
        final String masterTopicPath2 = "/meta/brokers/topics/xxtopic/1-m";
        final String slaveTopicPath = "/meta/brokers/topics/xxtopic/0-s1";
        final String masterTopicData = "2";
        final String masterTopicData2 = "3";
        final String slaveTopicData = "2";
        ZkUtils.createEphemeralPath(this.client, masterTopicPath, masterTopicData);
        ZkUtils.createEphemeralPath(this.client, masterTopicPath2, masterTopicData2);
        ZkUtils.createEphemeralPath(this.client, slaveTopicPath, slaveTopicData);

        // 旧版本的干扰数据 ,不受影响
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/xxtopic/0", masterTopicData);

        List<String> topics = Arrays.asList("xxtopic");
        Map<String, List<String>> ret = this.metaZookeeper.getPartitionStringsForTopics(topics);
        assertTrue(ret.size() == 1);
        assertTrue(ret.get("xxtopic").size() == 5);
        assertTrue(ret.get("xxtopic").contains("0-0"));
        assertTrue(ret.get("xxtopic").contains("0-1"));
        assertTrue(ret.get("xxtopic").contains("1-0"));
        assertTrue(ret.get("xxtopic").contains("1-1"));
        assertTrue(ret.get("xxtopic").contains("1-2"));

        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/0-m", "1");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/444", "2");// invalid
        topics = Arrays.asList("xxtopic", "yytopic");
        ret = this.metaZookeeper.getPartitionStringsForTopics(topics);
        assertTrue(ret.size() == 2);
        assertTrue(ret.get("xxtopic").size() == 5);
        assertTrue(ret.get("xxtopic").contains("0-0"));
        assertTrue(ret.get("xxtopic").contains("0-1"));
        assertTrue(ret.get("xxtopic").contains("1-0"));
        assertTrue(ret.get("xxtopic").contains("1-1"));
        assertTrue(ret.get("xxtopic").contains("1-2"));
        assertTrue(ret.get("yytopic").size() == 1);
        assertTrue(ret.get("yytopic").contains("0-0"));
    }


    @Test
    public void testGetPartitionsForTopicsFromMaster2() throws Exception {
        final String masterTopicPath = "/meta/brokers/topics/xxtopic/0-m";
        final String masterTopicPath2 = "/meta/brokers/topics/xxtopic/1-m";
        final String slaveTopicPath = "/meta/brokers/topics/xxtopic/0-s1";
        final String masterTopicData = "2";
        final String masterTopicData2 = "3";
        final String slaveTopicData = "4";
        ZkUtils.createEphemeralPath(this.client, masterTopicPath, masterTopicData);
        ZkUtils.createEphemeralPath(this.client, masterTopicPath2, masterTopicData2);
        ZkUtils.createEphemeralPath(this.client, slaveTopicPath, slaveTopicData);

        // 旧版本的干扰数据 ,不受影响
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/xxtopic/0", masterTopicData);

        List<String> topics = Arrays.asList("xxtopic");
        Map<String, List<Partition>> ret = this.metaZookeeper.getPartitionsForTopicsFromMaster(topics);
        assertTrue(ret.size() == 1);
        assertTrue(ret.get("xxtopic").size() == 5);
        assertTrue(ret.get("xxtopic").contains(new Partition("0-0")));
        assertTrue(ret.get("xxtopic").contains(new Partition("0-1")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-0")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-1")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-2")));

        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/0-m", "1");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/444", "2");// invalid
        topics = Arrays.asList("xxtopic", "yytopic");
        ret = this.metaZookeeper.getPartitionsForTopicsFromMaster(topics);
        assertTrue(ret.size() == 2);
        assertTrue(ret.get("xxtopic").size() == 5);
        assertTrue(ret.get("xxtopic").contains(new Partition("0-0")));
        assertTrue(ret.get("xxtopic").contains(new Partition("0-1")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-0")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-1")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-2")));
        assertTrue(ret.get("yytopic").size() == 1);
        assertTrue(ret.get("yytopic").contains(new Partition("0-0")));

    }


    @Test
    public void testGetPartitionsForTopicsFromMaster() throws Exception {
        final String masterTopicPath = "/meta/brokers/topics/xxtopic/0-m";
        final String masterTopicPath2 = "/meta/brokers/topics/xxtopic/1-m";
        final String slaveTopicPath = "/meta/brokers/topics/xxtopic/0-s1";
        final String masterTopicData = "2";
        final String masterTopicData2 = "3";
        final String slaveTopicData = "4";
        ZkUtils.createEphemeralPath(this.client, masterTopicPath, masterTopicData);
        ZkUtils.createEphemeralPath(this.client, masterTopicPath2, masterTopicData2);
        ZkUtils.createEphemeralPath(this.client, slaveTopicPath, slaveTopicData);

        // 旧版本的干扰数据 ,不受影响
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/xxtopic/0", masterTopicData);

        List<String> topics = Arrays.asList("xxtopic");

        Map<String, List<Partition>> ret = this.metaZookeeper.getPartitionsForTopicsFromMaster(topics, 0);
        assertTrue(ret.size() == 1);
        assertTrue(ret.get("xxtopic").size() == 2);
        assertTrue(ret.get("xxtopic").contains(new Partition("0-0")));
        assertTrue(ret.get("xxtopic").contains(new Partition("0-1")));

        ret = this.metaZookeeper.getPartitionsForTopicsFromMaster(topics, 1);
        assertTrue(ret.size() == 1);
        assertTrue(ret.get("xxtopic").size() == 3);
        assertTrue(ret.get("xxtopic").contains(new Partition("1-0")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-1")));
        assertTrue(ret.get("xxtopic").contains(new Partition("1-2")));

        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/0-m", "1");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/444", "2");// invalid
        topics = Arrays.asList("xxtopic", "yytopic");
        ret = this.metaZookeeper.getPartitionsForTopicsFromMaster(topics, 0);
        assertTrue(ret.size() == 2);
        assertTrue(ret.get("xxtopic").size() == 2);
        assertTrue(ret.get("xxtopic").contains(new Partition("0-0")));
        assertTrue(ret.get("xxtopic").contains(new Partition("0-1")));
        assertTrue(ret.get("yytopic").size() == 1);
        assertTrue(ret.get("yytopic").contains(new Partition("0-0")));

    }


    @Test
    public void testGetPartitionStringsForTopicsFromMaster() throws Exception {
        final String masterTopicPath = "/meta/brokers/topics/xxtopic/0-m";
        final String masterTopicPath2 = "/meta/brokers/topics/xxtopic/1-m";
        final String slaveTopicPath = "/meta/brokers/topics/xxtopic/0-s1";
        final String masterTopicData = "2";
        final String masterTopicData2 = "3";
        final String slaveTopicData = "4";
        ZkUtils.createEphemeralPath(this.client, masterTopicPath, masterTopicData);
        ZkUtils.createEphemeralPath(this.client, masterTopicPath2, masterTopicData2);
        ZkUtils.createEphemeralPath(this.client, slaveTopicPath, slaveTopicData);
        List<String> topics = Arrays.asList("xxtopic");

        Map<String, List<String>> ret = this.metaZookeeper.getPartitionStringsForTopicsFromMaster(topics, 0);
        assertTrue(ret.size() == 1);
        assertTrue(ret.get("xxtopic").size() == 2);
        assertTrue(ret.get("xxtopic").contains("0-0"));
        assertTrue(ret.get("xxtopic").contains("0-1"));

        ret = this.metaZookeeper.getPartitionStringsForTopicsFromMaster(topics, 1);
        assertTrue(ret.size() == 1);
        assertTrue(ret.get("xxtopic").size() == 3);
        assertTrue(ret.get("xxtopic").contains("1-0"));
        assertTrue(ret.get("xxtopic").contains("1-1"));
        assertTrue(ret.get("xxtopic").contains("1-2"));

        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/0-m", "1");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/444", "2");// invalid
        topics = Arrays.asList("xxtopic", "yytopic");
        ret = this.metaZookeeper.getPartitionStringsForTopicsFromMaster(topics, 0);
        assertTrue(ret.size() == 2);
        assertTrue(ret.get("xxtopic").size() == 2);
        assertTrue(ret.get("xxtopic").contains("0-0"));
        assertTrue(ret.get("xxtopic").contains("0-1"));
        assertTrue(ret.get("yytopic").size() == 1);
        assertTrue(ret.get("yytopic").contains("0-0"));

    }


    @Test
    public void testGetBrokersById() throws Exception {
        final String masterPath = "/meta/brokers/ids/1/master";
        String data = "meta://12.3.2.2:8123";
        ZkUtils.createEphemeralPath(this.client, masterPath, data);
        Set<Broker> ret = this.metaZookeeper.getBrokersById(1);
        assertTrue(ret.size() == 1);
        assertTrue(ret.contains(new Broker(1, data)));

        final String slavePath = "/meta/brokers/ids/1/slave1";
        data = "meta://12.3.2.3:8123";
        ZkUtils.createEphemeralPath(this.client, slavePath, data);
        ret = this.metaZookeeper.getBrokersById(1);
        assertTrue(ret.size() == 2);
        assertTrue(ret.contains(new Broker(1, "meta://12.3.2.2:8123")));
        assertTrue(ret.contains(new Broker(1, "meta://12.3.2.3:8123?slaveId=1")));
        ZkUtils.deletePath(this.client, masterPath);

        assertFalse(ZkUtils.pathExists(this.client, masterPath));
        ret = this.metaZookeeper.getBrokersById(1);
        assertTrue(ret.size() == 1);
        assertTrue(ret.contains(new Broker(1, "meta://12.3.2.3:8123?slaveId=1")));
    }


    @Test
    public void testGetMasterBrokersByTopic() throws Exception {
        final String brokerPath = "/meta/brokers/ids/0/master";
        final String brokerData = "meta://12.3.2.2:8123";
        ZkUtils.createEphemeralPath(this.client, brokerPath, brokerData);

        final String slaveBrokerPath = "/meta/brokers/ids/0/slave1";
        final String slaveBrokerData = "meta://12.3.2.2:8123";
        ZkUtils.createEphemeralPath(this.client, slaveBrokerPath, slaveBrokerData);

        final String brokerPath2 = "/meta/brokers/ids/1/master";
        final String brokerData2 = "meta://12.3.2.4:8123";
        ZkUtils.createEphemeralPath(this.client, brokerPath2, brokerData2);

        final String topicPath = "/meta/brokers/topics/xxtopic/0-m";
        final String slaveTopicPath = "/meta/brokers/topics/xxtopic/0-s1";
        final String topicPath2 = "/meta/brokers/topics/xxtopic/1-m";
        final String topicData = "2";
        ZkUtils.createEphemeralPath(this.client, topicPath, topicData);
        ZkUtils.createEphemeralPath(this.client, slaveTopicPath, topicData);
        ZkUtils.createEphemeralPath(this.client, topicPath2, topicData);

        final Map<Integer, String> map = this.metaZookeeper.getMasterBrokersByTopic("xxtopic");
        assertTrue(map.size() == 2);
        assertTrue(map.get(0).equals("meta://12.3.2.2:8123"));
        assertTrue(map.get(1).equals("meta://12.3.2.4:8123"));
    }


    @Test
    public void testGetTopicsByBrokerId_nodata() {
        final Set<String> set = this.metaZookeeper.getTopicsByBrokerIdFromMaster(55);
        assertTrue(set.size() == 0);
    }


    @Test
    public void testGetTopicsByBrokerId() throws Exception {
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/xxtopic/1-m", "2");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/xxtopic/1-s1", "2");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/1-m", "2");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/topics/yytopic/1-s1", "2");

        final Set<String> set = this.metaZookeeper.getTopicsByBrokerIdFromMaster(1);
        assertTrue(set.size() == 2);
        assertTrue(set.contains("xxtopic"));
        assertTrue(set.contains("yytopic"));

    }


    @Test
    public void testBrokerIdsPathOf() {
        String path = this.metaZookeeper.brokerIdsPathOf(1, 2);
        assertEquals("/meta/brokers/ids/1/slave2", path);

        path = this.metaZookeeper.brokerIdsPathOf(1, -2);
        assertEquals("/meta/brokers/ids/1/master", path);
    }


    @Test
    public void testBrokerTopicsPathOf() {
        String path = this.metaZookeeper.brokerTopicsPathOf("xxtopic", 1, 1);
        assertEquals("/meta/brokers/topics/xxtopic/1-s1", path);

        path = this.metaZookeeper.brokerTopicsPathOf("xxtopic", 1, -1);
        assertEquals("/meta/brokers/topics/xxtopic/1-m", path);
    }


    @Test
    public void testGetCluster() throws Exception {
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/ids/0/master", "meta://12.3.2.2:8123");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/ids/0/slave1", "meta://12.3.2.3:8123");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/ids/0/slave2", "meta://12.3.2.33:8123");
        ZkUtils.createEphemeralPath(this.client, "/meta/brokers/ids/1/master", "meta://12.3.2.4:8123");
        final Cluster cluster = this.metaZookeeper.getCluster();
        assertEquals(cluster.size(), 4);
        assertEquals(cluster.getMasterBroker(0).getZKString(), "meta://12.3.2.2:8123");
        assertEquals(cluster.getMasterBroker(1).getZKString(), "meta://12.3.2.4:8123");

        final Cluster cluster2 = new Cluster();
        cluster2.addBroker(0, new Broker(0, "meta://12.3.2.2:8123"));
        cluster2.addBroker(0, new Broker(0, "meta://12.3.2.3:8123?slaveId=1"));
        cluster2.addBroker(0, new Broker(0, "meta://12.3.2.33:8123?slaveId=2"));
        cluster2.addBroker(1, new Broker(1, "meta://12.3.2.4:8123"));
        assertTrue(cluster2.equals(cluster));

    }


    @Test
    public void testSetupPartitionGetPartitionsForTopicsGetCluster() throws Exception {
        this.metaZookeeper.setupPartition(1, "localhost", 8123, "topic1", 1, -1);
        this.metaZookeeper.setupPartition(2, "localhost", 8124, "topic1", 2, -1);
        this.metaZookeeper.setupPartition(3, "localhost", 8125, "topic2", 3, -1);
        this.metaZookeeper.setupPartition(4, "localhost", 8126, "topic2", 4, -1);
        this.metaZookeeper.setupPartition(5, "localhost", 8127, "topic3", 5, -1);

        final List<String> topics = new ArrayList<String>();
        topics.add("topic1");
        topics.add("topic2");
        topics.add("topic3");
        final Map<String/* topic */, List<Partition>> topicParts =
                this.metaZookeeper.getPartitionsForTopicsFromMaster(topics);

        assertEquals(3, topicParts.size());
        assertTrue(topicParts.containsKey("topic1"));
        assertTrue(topicParts.containsKey("topic2"));
        assertTrue(topicParts.containsKey("topic3"));

        final List<Partition> parts1 = topicParts.get("topic1");
        assertEquals(3, parts1.size());
        assertTrue(parts1.contains(new Partition("1-0")));
        assertTrue(parts1.contains(new Partition("2-0")));
        assertTrue(parts1.contains(new Partition("2-1")));

        final List<Partition> parts2 = topicParts.get("topic2");
        assertEquals(7, parts2.size());
        assertTrue(parts2.contains(new Partition("3-0")));
        assertTrue(parts2.contains(new Partition("3-1")));
        assertTrue(parts2.contains(new Partition("3-2")));
        assertTrue(parts2.contains(new Partition("4-0")));
        assertTrue(parts2.contains(new Partition("4-1")));
        assertTrue(parts2.contains(new Partition("4-2")));
        assertTrue(parts2.contains(new Partition("4-3")));

        final List<Partition> parts3 = topicParts.get("topic3");
        assertEquals(5, parts3.size());
        assertTrue(parts3.contains(new Partition("5-0")));
        assertTrue(parts3.contains(new Partition("5-1")));
        assertTrue(parts3.contains(new Partition("5-2")));
        assertTrue(parts3.contains(new Partition("5-3")));
        assertTrue(parts3.contains(new Partition("5-4")));

        this.metaZookeeper.deletePartition(1, "test1", -1);
        this.metaZookeeper.deletePartition(2, "test1", -1);
        this.metaZookeeper.deletePartition(3, "test2", -1);
        this.metaZookeeper.deletePartition(4, "test2", -1);
        this.metaZookeeper.deletePartition(5, "test3", -1);
    }


    @After
    public void tearDown() throws Exception {
        this.diamondManager.close();
        this.client.close();
    }

}
