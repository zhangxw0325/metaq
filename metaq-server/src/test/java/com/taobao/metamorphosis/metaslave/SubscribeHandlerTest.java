package com.taobao.metamorphosis.metaslave;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ZkUtils;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-7-1 ÏÂÎç05:08:33
 */

public class SubscribeHandlerTest {
    private MetaConfig metaConfig;
    private MetaMorphosisBroker broker;
    private SubscribeHandler subscribeHandler;
    private MetaZookeeper metaZookeeper;
    private final int brokerId = 999;


    @Before
    public void setup() throws Exception {
        this.metaConfig = new MetaConfig();
        this.metaConfig.setDiamondZKDataId("metamorphosis.testZkConfig");
        this.metaConfig.setBrokerId(this.brokerId);
        this.metaConfig.setHostName("localhost");
        this.metaConfig.setServerPort(8199);
        this.broker = new MetaMorphosisBroker(this.metaConfig);
        this.subscribeHandler = new SubscribeHandler(this.broker);
        this.metaZookeeper = this.broker.getBrokerZooKeeper().getMetaZookeeper();
    }


    @After
    public void tearDown() {
        this.subscribeHandler.stop();

    }


    @Test
    public void testStart_NoTopicsOfMasterInZk() {
        Assert.assertEquals(0, this.subscribeHandler.getSlaveZooKeeper().getPartitionsForTopicsFromMaster().size());
        this.subscribeHandler.start();
        Assert.assertFalse(this.subscribeHandler.isStarted());
    }


    @Test
    public void testStart_NoTopicsOfMasterInZk_thenMasterRegister() throws Exception {
        Assert.assertEquals(0, this.subscribeHandler.getSlaveZooKeeper().getPartitionsForTopicsFromMaster().size());
        this.subscribeHandler.start();
        Assert.assertFalse(this.subscribeHandler.isStarted());
        ZkUtils.createEphemeralPath(this.getZkClient(), this.metaZookeeper.brokerIdsPathOf(this.brokerId, -1),
            "meta://1.1.1.1:222");
        ZkUtils.createEphemeralPath(this.getZkClient(),
            this.metaZookeeper.brokerTopicsPathOf("topictest", this.brokerId, -1), "2");

        Thread.sleep(5000);
    }


    @Test
    public void testStart_MasterNoStarted() throws Exception {
        ZkUtils.deletePath(this.getZkClient(), this.metaZookeeper.brokerIdsPathOf(this.brokerId, -1));
        ZkUtils.deletePath(this.getZkClient(), this.metaZookeeper.brokerTopicsPathOf("topictest", this.brokerId, -1));

        ZkUtils.createEphemeralPath(this.getZkClient(), this.metaZookeeper.brokerIdsPathOf(this.brokerId, -1),
            "meta://1.1.1.1:222");
        ZkUtils.createEphemeralPath(this.getZkClient(),
            this.metaZookeeper.brokerTopicsPathOf("topictest", this.brokerId, -1), "2");
        Assert.assertTrue(this.subscribeHandler.getSlaveZooKeeper().getPartitionsForTopicsFromMaster().size() > 0);
        this.subscribeHandler.start();
        Assert.assertFalse(this.subscribeHandler.isStarted());
    }


    private ZkClient getZkClient() {
        return this.broker.getBrokerZooKeeper().getZkClient();
    }
}
