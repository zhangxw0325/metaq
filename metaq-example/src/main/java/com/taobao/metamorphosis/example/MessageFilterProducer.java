package com.taobao.metamorphosis.example;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;

public class MessageFilterProducer {
	public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(new MetaClientConfig());
        // create producer
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = "meta-test-20";
        producer.publish(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while (true) {
        	System.out.println("type message content:");
        	line = reader.readLine();
        	Message message = new Message(topic, line.getBytes()); 
        	System.out.println("type message filter:");
        	line = reader.readLine();
        	message.setAttribute(line);
            // send message
            final SendResult sendResult = producer.sendMessage(message);
            // check result
            if (!sendResult.isSuccess()) {
                System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
            }
            else {
                System.out.println("Send message successfully,sent to " + sendResult.getPartition());
            }
        }
    }

    
}
