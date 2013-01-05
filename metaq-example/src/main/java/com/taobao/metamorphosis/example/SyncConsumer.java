/**
 * $Id: SyncConsumer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.example;

import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.DequeueResult;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.cluster.Partition;


public class SyncConsumer {
    private static void printMessage(List<Message> msgList) {
        for (Message msg : msgList) {
            System.out.println(msg.getMsgNewId());
        }
    }


    public static void main(String[] args) throws Exception {
        // New session factory
        MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(new MetaClientConfig());
        // subscribed topic
        final String topic = "meta-test-20";
        // consumer group
        final String group = "SyncConsumer2";
        // create consumer
        MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));

        // start offset
        long offset = 0;
        for (;;) {
            DequeueResult result = consumer.dequeue(topic, new Partition("710-0"), offset, 1024 * 1024);
            if (result != null) {
                switch (result.getStatus()) {
                case STATUS_OK:
                    printMessage(result.getMsgList());
                    offset = result.getLastMsgOffset() + 1;
                    break;
                case STATUS_NOT_FOUND:
                    Thread.sleep(1000 * 5);
                    break;
                case STATUS_MOVED:
                    offset = result.getMovedOffset();
                    break;
                case STATUS_OTHER_ERROR:
                    Thread.sleep(1000 * 5);
                    break;
                }

                System.out.println("dequeue result " + result.getStatus());
            }
        }
    }
}
