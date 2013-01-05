package com.taobao.metamorphosis.example;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;


/**
 * 非auto ack模式下接收消息的例子
 * 
 * @author boyan
 * 
 */
public class TransactionalConsumer {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(new MetaClientConfig());
        // subscribed topic
        final String topic = "meta-test";
        // consumer group
        final String group = "meta-example";
        // create consumer
        final MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
        // subscribe topic
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {

            private int count = 0;


            public void recieveMessages(final Message message) {
                System.out.println("Receive message " + new String(message.getData()));
                // set auto ack to false
                message.getPartition().setAutoAck(false);
                // ack once per two messages
                if (++this.count % 2 == 0) {
                    message.getPartition().ack();
                    System.out.println("ack message");
                }
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
