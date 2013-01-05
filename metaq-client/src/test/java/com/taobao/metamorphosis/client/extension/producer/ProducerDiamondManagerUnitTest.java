package com.taobao.metamorphosis.client.extension.producer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-12-31 ÏÂÎç1:33:08
 */

public class ProducerDiamondManagerUnitTest {

    ProducerDiamondManager producerDiamondManager;

    DiamondManager partitionsDiamondManager;
    IMocksControl mocksControl;


    @Before
    public void setUp() throws Exception {
        mocksControl = EasyMock.createControl();
        partitionsDiamondManager = mocksControl.createMock(DiamondManager.class);

    }


    @Test
    public void testProducerDiamondManager() {
        producerDiamondManager = new ProducerDiamondManager(partitionsDiamondManager);
    }


    @Test
    public void testGetPartitions_nullProperties() {
        EasyMock.expect(partitionsDiamondManager.getAvailablePropertiesConfigureInfomation(10000)).andReturn(null);
        mocksControl.replay();
        producerDiamondManager = new ProducerDiamondManager(partitionsDiamondManager);
        Map<String, List<Partition>> map = producerDiamondManager.getPartitions();
        Assert.assertTrue(map != null);
        Assert.assertEquals(0, map.size());
        mocksControl.verify();
    }


    @Test
    public void testGetPartitions_emptyProperties() {
        EasyMock.expect(partitionsDiamondManager.getAvailablePropertiesConfigureInfomation(10000)).andReturn(
            new Properties());
        mocksControl.replay();
        producerDiamondManager = new ProducerDiamondManager(partitionsDiamondManager);
        Map<String, List<Partition>> map = producerDiamondManager.getPartitions();
        Assert.assertTrue(map != null);
        Assert.assertEquals(0, map.size());
        mocksControl.verify();
    }


    @Test
    public void testGetPartitions() {
        Properties properties = new Properties();
        properties.put("topic.num.test-topic", "0:2;1:2");
        properties.put("topic.num.test-topic2", "0:3;1:4");

        EasyMock.expect(partitionsDiamondManager.getAvailablePropertiesConfigureInfomation(10000))
            .andReturn(properties);
        mocksControl.replay();
        producerDiamondManager = new ProducerDiamondManager(partitionsDiamondManager);
        Map<String, List<Partition>> map = producerDiamondManager.getPartitions();
        Assert.assertTrue(map != null);
        Assert.assertEquals(2, map.size());

        Assert.assertEquals(4, map.get("test-topic").size());
        Assert.assertTrue(map.get("test-topic").contains(new Partition(0, 0)));
        Assert.assertTrue(map.get("test-topic").contains(new Partition(0, 1)));
        Assert.assertTrue(map.get("test-topic").contains(new Partition(1, 0)));
        Assert.assertTrue(map.get("test-topic").contains(new Partition(1, 1)));

        Assert.assertEquals(7, map.get("test-topic2").size());
        Assert.assertTrue(map.get("test-topic2").contains(new Partition(0, 0)));
        Assert.assertTrue(map.get("test-topic2").contains(new Partition(0, 1)));
        Assert.assertTrue(map.get("test-topic2").contains(new Partition(0, 2)));
        Assert.assertTrue(map.get("test-topic2").contains(new Partition(1, 0)));
        Assert.assertTrue(map.get("test-topic2").contains(new Partition(1, 1)));
        Assert.assertTrue(map.get("test-topic2").contains(new Partition(1, 2)));
        Assert.assertTrue(map.get("test-topic2").contains(new Partition(1, 3)));
        mocksControl.verify();
    }
}
