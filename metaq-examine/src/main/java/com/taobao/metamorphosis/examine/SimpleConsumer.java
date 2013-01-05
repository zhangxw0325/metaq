/**
 * $Id: SimpleConsumer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.examine;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;


public class SimpleConsumer {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(buildMetaClientConfig());
        // subscribed topic
        final String topic = "TOPIC_HELLO_WORLD";
        // consumer group
        final String group = "TOPIC_HELLO_WORLD-" + System.currentTimeMillis();
        // create consumer
        final MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
        // subscribe topic
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {
            AtomicLong msgIndex = new AtomicLong(0);


            public void recieveMessages(final Message message) {
                System.out.println(this.msgIndex.incrementAndGet() + "\tReceived message, topic: "
                        + message.getTopic() + " Id: " + message.getMsgNewId());
            }


            public Executor getExecutor() {
                return null;
            }
        });

        // complete subscribe
        consumer.completeSubscribe();
    }


    private static MetaClientConfig buildMetaClientConfig() {
        MetaClientConfig config = new MetaClientConfig();
        return config;
    }

}
