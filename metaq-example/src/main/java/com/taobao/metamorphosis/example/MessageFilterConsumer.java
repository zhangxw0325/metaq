package com.taobao.metamorphosis.example;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;

public class MessageFilterConsumer {
	public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(buildMetaClientConfig());
        // subscribed topic
        final String topic = "meta-test-20";
        // consumer group
        final String group = "meta-vintage-x-4";
        // create consumer
        final MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
        
        String[] types = {"type1","type2"};
        // subscribe topic
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            public void recieveMessages(final Message message) {
                System.out.println("newId:" + message.getMsgNewId() + "\t attribute is:" + message.getAttribute());
            }

            public Executor getExecutor() {
                return null;
            }
        }, types);
        // complete subscribe
        consumer.completeSubscribe();
    }


    private static MetaClientConfig buildMetaClientConfig() {
        MetaClientConfig config = new MetaClientConfig();
        return config;
    }
}
