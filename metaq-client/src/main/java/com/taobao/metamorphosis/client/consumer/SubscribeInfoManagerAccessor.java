package com.taobao.metamorphosis.client.consumer;

import java.util.concurrent.ConcurrentHashMap;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-11-7 ÏÂÎç3:03:51
 */

public class SubscribeInfoManagerAccessor {

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>> getGroupTopicSubcriberRegistry(
            final SubscribeInfoManager subscribeInfoManager) {
        return subscribeInfoManager.getGroupTopicSubcriberRegistry();
    }
}
