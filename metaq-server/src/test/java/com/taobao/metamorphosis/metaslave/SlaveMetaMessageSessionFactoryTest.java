package com.taobao.metamorphosis.metaslave;

import org.junit.Test;

import com.taobao.metamorphosis.client.MetaClientConfig;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-7-1 ÏÂÎç04:20:01
 */

public class SlaveMetaMessageSessionFactoryTest {

    @Test
    public void testCreate() throws Exception {
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        SlaveMetaMessageSessionFactory.create(metaClientConfig, 100);
    }
}
