/**
 * $Id: MessageTypeConsumer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.examine;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metaq.commons.MetaUtil;


public class MessageTypeConsumer {
    private static AtomicLong receiveMsgTotal = new AtomicLong(0);


    public static void main(final String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("./consumer.sh topic");
            System.exit(-1);
        }

        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(buildMetaClientConfig());
        // subscribed topic
        final String topic = args[0];
        // consumer group
        final String group = topic + "-" + System.currentTimeMillis() / 100000;
        // create consumer
        final MessageConsumer consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
        final String[] types = new String[500];
        for(int i = 0; i < 500; i++){
        	StringBuilder s = new StringBuilder();
        	for(int j = 0; j < 10; j++){
        		s.append(i);
        	}
        	types[i] = s.toString();
        }
        // subscribe topic
        consumer.subscribe(topic, 1024 * 1024, new MessageListener() {
            public void recieveMessages(final Message message) {
                receiveMsgTotal.incrementAndGet();
            }


            public Executor getExecutor() {
                return null;
            }
        }, types);

        // complete subscribe
        consumer.completeSubscribe();

        System.out.println("consumer group = " + group);

        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println(MetaUtil.timeMillisToHumanString(System.currentTimeMillis())
                        + " receiveMsgTotal " + receiveMsgTotal.get());
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }


    private static MetaClientConfig buildMetaClientConfig() {
        MetaClientConfig config = new MetaClientConfig();
        return config;
    }

}
