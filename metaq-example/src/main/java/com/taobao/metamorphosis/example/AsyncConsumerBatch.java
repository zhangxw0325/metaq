/**
 * $Id: AsyncConsumerBatch.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.example;

import java.util.List;
import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListListener;


/**
 * 异步消息消费者
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class AsyncConsumerBatch {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(new MetaClientConfig());
        // subscribed topic
        final String topic = "meta-test-20";
        // consumer group
        final String group = "meta-example-x-3";
        // create consumer
        final MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
        // subscribe topic
        consumer.subscribe(topic, 1024 * 1024, new MessageListListener() {

            @Override
            public void recieveMessages(Message message) {
            }


            @Override
            public Executor getExecutor() {
                return null;
            }


            @Override
            public void recieveMessageList(List<Message> msgs) {
                for (Message msg : msgs) {
                    System.out.println(msg.getMsgNewId());
                }

                System.out.println("receive batch message " + msgs.size());
            }
        });

        // complete subscribe
        consumer.completeSubscribe();
    }

}
