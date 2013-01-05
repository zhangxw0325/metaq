package com.taobao.metamorphosis.client.consumer.storage;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;

import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.utils.DiamondUtils;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.MetaZookeeper.ZKGroupTopicDirs;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class ZkOffsetStorageUnitTest {

    private ZkClient client;
    private DiamondManager diamondManager;
    private ZKConfig zkConfig;
    private ZkOffsetStorage offsetStorage;
    private MetaZookeeper metaZookeeper;


    @Before
    public void setUp() {
        this.diamondManager = new DefaultDiamondManager(null, "metamorphosis.testZkConfig", (ManagerListener) null);
        this.zkConfig = DiamondUtils.getZkConfig(this.diamondManager, 10000);
        this.client =
                new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
        this.metaZookeeper = new MetaZookeeper(this.client, this.zkConfig.zkRoot);
        this.offsetStorage = new ZkOffsetStorage(this.metaZookeeper, this.client);
        this.client.deleteRecursive("/meta/consumers");
    }


    @Test
    public void testCompatibleWithOldClient() throws Exception {
        // 测试是否与老客户端兼容
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        final String topic = "test";
        final ZKGroupTopicDirs topicDirs = this.metaZookeeper.new ZKGroupTopicDirs(topic, group);

        final long offset = 1999;
        ZkUtils.updatePersistentPath(this.client, topicDirs.consumerOffsetDir + "/" + partition.toString(),
            String.valueOf(offset));

        final TopicPartitionRegInfo info = this.offsetStorage.load(topic, group, partition);
        assertEquals(topic, info.getTopic());
        assertEquals(-1L, info.getMessageId());
        assertEquals(partition, info.getPartition());
        assertEquals(offset, info.getOffset().get());
    }


    @Test
    public void testCommitLoadOffsets() {
        final String group = "test-grp";
        final Partition partition = new Partition("0-1");
        final Collection<TopicPartitionRegInfo> infoList = new ArrayList<TopicPartitionRegInfo>();
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            final TopicPartitionRegInfo info = new TopicPartitionRegInfo(topic, partition, i, i + 1);
            info.setModified(true);
            infoList.add(info);

        }
        this.offsetStorage.commitOffset(group, infoList);
        for (int i = 0; i < 3; i++) {
            final String topic = "test" + (i + 1);
            final TopicPartitionRegInfo info = this.offsetStorage.load(topic, "test-grp", partition);
            assertEquals(topic, info.getTopic());
            assertEquals(i + 1, info.getMessageId());
            assertEquals(partition, info.getPartition());
            assertEquals(i, info.getOffset().get());
            info.getOffset().set(i);
            infoList.add(info);
        }
    }


    @After
    public void tearDown() {
        if (this.client != null) {
            this.client.deleteRecursive("/meta/consumers");
        }
        if (this.diamondManager != null) {
            this.diamondManager.close();
        }
        if (this.client != null) {
            this.client.close();
        }
    }

}
