package com.taobao.metamorphosis.example;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.extension.BroadcastMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.MetaBroadcastMessageSessionFactory;


/**
 * 广播接收
 * 
 * @author 无花
 * @since 2012-2-22 下午4:24:08
 */

public class BroadcastAsyncConsumer {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final BroadcastMessageSessionFactory sessionFactory =
                new MetaBroadcastMessageSessionFactory(new MetaClientConfig());

        // subscribed topic
        final String topic = "slave-test";
        // consumer group
        final String group = "meta-example";
        // create consumer
        final MessageConsumer consumer = sessionFactory.createBroadcastConsumer(new ConsumerConfig(group));
        // subscribe topic
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            public void recieveMessages(final Message message) {
                System.out.println("Receive message " + new String(message.getData()));
            }


            public Executor getExecutor() {
                // Thread pool to process messages,maybe null.
                return null;
            }
        });
        // complete subscribe
        consumer.completeSubscribe();

    }
}
