package com.taobao.metamorphosis.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.extension.OrderedMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.OrderedMetaMessageSessionFactory;
import com.taobao.metamorphosis.client.extension.producer.OrderedMessagePartitionSelector;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 发送顺序消息，需要同时在diamond中配置分区分布情况
 * 
 * @author 无花
 * @since 2012-2-22 下午4:28:11
 */

public class OrderedProducer {
    public static void main(final String[] args) throws Exception {

        // New session factory
        final OrderedMessageSessionFactory sessionFactory =
                new OrderedMetaMessageSessionFactory(new MetaClientConfig());

        // create producer
        final MessageProducer producer = sessionFactory.createProducer(new CustomPartitionSelector());

        // publish topic
        final String topic = "meta-test";
        producer.publish(topic);

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        while ((line = readLine(reader)) != null) {
            // send message
            final SendResult sendResult = producer.sendMessage(new Message(topic, line.getBytes()));
            // check result
            if (!sendResult.isSuccess()) {
                System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
            }
            else {
                System.out.println("Send message successfully,sent to " + sendResult.getPartition());
            }
        }
    }


    private static String readLine(final BufferedReader reader) throws IOException {
        System.out.println("Type a message to send:");
        return reader.readLine();
    }

    static class CustomPartitionSelector extends OrderedMessagePartitionSelector {

        @Override
        protected Partition choosePartition(final String topic, final List<Partition> partitions, final Message message) {
            // 根据一定的规则把需要有序的局部消息路由到同一个分区
            final int hashCode = new String(message.getData()).hashCode();
            final int partitionNo = hashCode % partitions.size();
            return partitions.get(partitionNo);
        }
    }
}
