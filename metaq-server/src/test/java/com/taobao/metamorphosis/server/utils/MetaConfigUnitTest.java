package com.taobao.metamorphosis.server.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.utils.ResourceUtils;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-22 ÏÂÎç03:21:49
 */

public class MetaConfigUnitTest {

    @Test
    public void testIsSlave() {
        MetaConfig metaConfig = new MetaConfig();
        metaConfig.setSlaveId(1);
        Assert.assertTrue(metaConfig.isSlave());

        metaConfig = new MetaConfig();
        metaConfig.setSlaveId(-2);
        Assert.assertFalse(metaConfig.isSlave());

        Assert.assertFalse(new MetaConfig().isSlave());
    }


    @Test
    public void testIsSlave_LoadProperty() throws Exception {
        MetaConfig metaConfig = new MetaConfig();
        this.LoadProperty(metaConfig, "master_brokerIdEmpty.ini");
        Assert.assertEquals(-1, metaConfig.getSlaveId());
        Assert.assertFalse(metaConfig.isSlave());

        metaConfig = new MetaConfig();
        this.LoadProperty(metaConfig, "master_noBrokerId.ini");
        Assert.assertEquals(-1, metaConfig.getSlaveId());
        Assert.assertFalse(metaConfig.isSlave());

        // metaConfig = new MetaConfig();
        // this.LoadProperty(metaConfig, "slave.ini");
        // Assert.assertEquals(0, metaConfig.getSlaveId());
        // Assert.assertTrue(metaConfig.isSlave());
    }


    private void LoadProperty(MetaConfig metaConfig, String fileName) throws IOException {
        metaConfig.loadRootConfig(ResourceUtils.getResourceAsFile(
            this.getClass().getPackage().getName().replaceAll("\\.", "/") + "/" + fileName).getAbsolutePath());
    }


    @Test
    public void testClosePartitions_topicNotPublished() {
        MetaConfig metaConfig = new MetaConfig();
        metaConfig.closePartitions("topic1", 1, 2);
        assertFalse(metaConfig.isClosedPartition("topic1", 1));
    }


    @Test
    public void testClosePartitions() {
        MetaConfig metaConfig = new MetaConfig();
        metaConfig.setTopics(Arrays.asList("topic1"));
        metaConfig.closePartitions("topic1", 1, 2);
        assertTrue(metaConfig.isClosedPartition("topic1", 1));
    }
}
