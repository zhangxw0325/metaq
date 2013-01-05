package com.taobao.metamorphosis.client.consumer;

import java.util.concurrent.ConcurrentHashMap;

import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 订阅信息管理器
 */
public class SubscribeInfoManager {
    private final ConcurrentHashMap<String/* group */, ConcurrentHashMap<String/* topic */, SubscriberInfo>> groupTopicSubcriberRegistry =
            new ConcurrentHashMap<String/* group */, ConcurrentHashMap<String, SubscriberInfo>>();


    public void subscribe(final String topic, final String group, final int maxSize,
            final MessageListener messageListener) throws MetaClientException {
        final ConcurrentHashMap<String, SubscriberInfo> topicSubsriberRegistry = this.getTopicSubscriberRegistry(group);
        SubscriberInfo info = topicSubsriberRegistry.get(topic);
        if (info == null) {
            info = new SubscriberInfo(messageListener, maxSize, null);
            final SubscriberInfo oldInfo = topicSubsriberRegistry.putIfAbsent(topic, info);
            if (oldInfo != null) {
                throw new MetaClientException("Topic=" + topic + " has been subscribered by group " + group);
            }
        }
        else {
            throw new MetaClientException("Topic=" + topic + " has been subscribered by group " + group);
        }
    }


    private ConcurrentHashMap<String, SubscriberInfo> getTopicSubscriberRegistry(final String group)
            throws MetaClientException {
        ConcurrentHashMap<String/* topic */, SubscriberInfo> topicSubsriberRegistry =
                this.groupTopicSubcriberRegistry.get(group);
        if (topicSubsriberRegistry == null) {
            topicSubsriberRegistry = new ConcurrentHashMap<String, SubscriberInfo>();
            final ConcurrentHashMap<String/* topic */, SubscriberInfo> old =
                    this.groupTopicSubcriberRegistry.putIfAbsent(group, topicSubsriberRegistry);
            if (old != null) {
                topicSubsriberRegistry = old;
            }
        }
        return topicSubsriberRegistry;
    }


    public MessageListener getMessageListener(final String topic, final String group) throws MetaClientException {
        final ConcurrentHashMap<String, SubscriberInfo> topicSubsriberRegistry =
                this.groupTopicSubcriberRegistry.get(group);
        if (topicSubsriberRegistry == null) {
            return null;
        }
        final SubscriberInfo info = topicSubsriberRegistry.get(topic);
        if (info == null) {
            return null;
        }
        return info.getMessageListener();
    }


    public void removeGroup(final String group) {
        this.groupTopicSubcriberRegistry.remove(group);
    }


    ConcurrentHashMap<String, ConcurrentHashMap<String, SubscriberInfo>> getGroupTopicSubcriberRegistry() {
        return this.groupTopicSubcriberRegistry;
    }
}
