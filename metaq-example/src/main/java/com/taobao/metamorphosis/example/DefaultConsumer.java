/**
 * $Id: DefaultConsumer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.example;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;


public class DefaultConsumer {
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
                System.out.println(message.getMsgNewId());
//                System.out.println("Receive message: " + new String(message.getData()) + ", ID: " + message.getId() + ", attribute: "
//                        + message.getAttribute() + ", DIFF: " + (System.currentTimeMillis() - Long.parseLong(message.getAttribute())));
            }


            public Executor getExecutor() {
                return null;
            }
        }, types);
        // complete subscribe
        consumer.completeSubscribe();
        System.out.println("1111");

    }


    private static MetaClientConfig buildMetaClientConfig() {
        MetaClientConfig config = new MetaClientConfig();
        // config.setDiamondZKDataId("meta.debug.zkConfig");
        return config;
    }

}
