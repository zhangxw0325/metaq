package com.taobao.metamorphosis.client.producer;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


public class RoundRobinPartitionSelectorUnitTest {

    private RoundRobinPartitionSelector selector;


    @Before
    public void setUp() {
        this.selector = new RoundRobinPartitionSelector();
    }


    @Test(expected = MetaClientException.class)
    public void testSelect_EmptyList() throws Exception {
        assertNull(this.selector.getPartition("test", null, null));
    }


    @Test
    public void testSelectRoundRobin() throws Exception {
        final Partition p1 = new Partition("0-1");
        final Partition p2 = new Partition("0-2");
        final Partition p3 = new Partition("0-3");

        final List<Partition> list = new ArrayList<Partition>();
        list.add(p1);
        list.add(p2);
        list.add(p3);

        assertSame(p2, this.selector.getPartition("test", list, null));
        assertSame(p3, this.selector.getPartition("test", list, null));
        assertSame(p1, this.selector.getPartition("test", list, null));
        assertSame(p2, this.selector.getPartition("test", list, null));
        assertSame(p3, this.selector.getPartition("test", list, null));
        assertSame(p1, this.selector.getPartition("test", list, null));
    }

}
