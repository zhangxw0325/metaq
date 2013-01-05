package com.taobao.metamorphosis.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;


/**
 * 发送者本地事务的简单例子
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-26
 * 
 */
public class TransactionProducer {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(new MetaClientConfig());
        // create producer
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = "meta-test";
        producer.publish(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while ((line = readLine(reader)) != null) {
            try {
                // 开始事务
                producer.beginTransaction();
                // 在事务内发送两条消息
                if (!producer.sendMessage(new Message(topic, line.getBytes())).isSuccess()) {
                    // 发送失败，立即回滚
                    producer.rollback();
                    continue;
                }
                if (!producer.sendMessage(new Message(topic, line.getBytes())).isSuccess()) {
                    producer.rollback();
                    continue;
                }
                // 提交
                producer.commit();

            }
            catch (final Exception e) {
                producer.rollback();
            }
        }
    }


    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type message to send:");
        return reader.readLine();
    }
}
