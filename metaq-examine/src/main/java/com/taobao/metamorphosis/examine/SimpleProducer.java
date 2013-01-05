/**
 * $Id: SimpleProducer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.examine;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;


public class SimpleProducer {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(buildMetaClientConfig());
        // create producer
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = "TOPIC_HELLO_WORLD";
        producer.publish(topic);

        // Message
        final Message msg = buildMessage(topic);

        for (int i = 0; i < 100; i++) {
            SendResult sendResult = producer.sendMessage(msg);
            if (!sendResult.isSuccess()) {
                System.err.println(i + "\tSend message failed, error message:" + sendResult.getErrorMessage());
                Thread.sleep(1000);
            }
            else {
                System.out.println("send ok " + i + " " + msg.getMsgNewId());
            }
        }

    }


    private static MetaClientConfig buildMetaClientConfig() {
        MetaClientConfig config = new MetaClientConfig();
        return config;
    }


    private static Message buildMessage(String topic) {
        StringBuilder sb = new StringBuilder();
        final int BodySize = 1024 * 2;
        for (int i = 0; i < BodySize; i++) {
            sb.append("K");
        }

        return new Message(topic, sb.toString().getBytes(), Long.toString(System.currentTimeMillis()));
    }
}
