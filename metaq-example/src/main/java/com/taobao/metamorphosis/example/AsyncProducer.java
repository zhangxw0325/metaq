package com.taobao.metamorphosis.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendMessageCallback;
import com.taobao.metamorphosis.client.producer.SendResult;


/**
 * 异步消息发送者
 * 
 * @author 无花
 * @Date 2012-2-27
 * 
 */
public class AsyncProducer {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setDiamondZKDataId("metamorphosis.testZkConfig");
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        // create producer
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = "slave-test";
        producer.publish(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while ((line = readLine(reader)) != null) {
            // send message
            try {
                producer.sendMessage(new Message(topic, line.getBytes()), new SendMessageCallback() {

                    public void onMessageSent(final SendResult result) {
                        if (result.isSuccess()) {
                            System.out.println("Send message successfully,sent to " + result.getPartition());

                        }
                        else {
                            System.err.println("Send message failed,error message:" + result.getErrorMessage());
                        }

                    }


                    public void onException(final Throwable e) {
                        e.printStackTrace();

                    }
                });

            }
            catch (final Exception e) {
                e.printStackTrace();
            }
            // check result
        }
    }


    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type a message to send:");
        return reader.readLine();
    }
}
