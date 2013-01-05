/**
 * $Id: MessageTypeProducer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.examine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;


public class MessageTypeProducer {
    private static int MsgBodySize = 1024;

    public static void main(String[] args) throws Exception {
        if (args.length <= 3) {
            System.out.println("./producer.sh topic theadcnt msgsize printMax");
            System.exit(-1);
        }
        args = new String[4];
        args[0] = "pingwei_test_x3";
        args[1] = "1";
        args[2] = "128";
        args[3] = "100";
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(buildMetaClientConfig());
        // create producer
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = args[0];
        producer.publish(topic);

        final int ThreadSize = Integer.parseInt(args[1]);

        MsgBodySize = Integer.parseInt(args[2]);

        // Thread pool
        final ThreadPoolExecutor executorSend = (ThreadPoolExecutor) Executors.newFixedThreadPool(ThreadSize);
        final List<Message> mlist = new ArrayList<Message>();
        final int typeCount = 1000;
        for(int i = 0; i < typeCount ;i ++){
        	Message m = buildMessage(topic);
        	StringBuilder s = new StringBuilder();
        	for(int j =0; j< 10; j++){
        		s.append(i);
        	}
        	m.setAttribute(s.toString());
        	mlist.add(m);
        }
        final int PrintMax ;
        if(args.length >= 4){
        	PrintMax = Integer.parseInt(args[3]);
        } else {
        	PrintMax = 10000;
        }
        final int count = PrintMax*20;
        final AtomicLong maxResponseTime = new AtomicLong(0);
        final AtomicLong over5Times = new AtomicLong(0);
        for (int i = 0; i < ThreadSize; i++) {
            executorSend.execute(new Runnable() {

                @Override
                public void run() {
                    long lastTime = System.currentTimeMillis();

                    for (int k = 1;k < count; k++) {
                        try {
                            long beginTime = System.currentTimeMillis();
                            SendResult sendResult = producer.sendMessage(mlist.get(k % typeCount));
                            long rt = System.currentTimeMillis() - beginTime;
                            if (rt > maxResponseTime.get()) {
                                System.out.println("pre maxRT " + maxResponseTime.get() + " currentRT " + rt);
                                maxResponseTime.set(rt);
                            }

                            if (rt > 3000) {
                                System.out.println("send RT over 5sec " + rt + " " + over5Times.incrementAndGet());
                            }

                            if (!sendResult.isSuccess()) {
                                System.err.println(k + "\tSend message failed, error message:"
                                        + sendResult.getErrorMessage());
                                Thread.sleep(1000);
                            }
                            else {
                                if ((k % PrintMax) == 0) {
                                    long eclipseTime = System.currentTimeMillis() - lastTime;
                                    double tps = PrintMax / (eclipseTime * 0.001);
                                    System.out.println(k + "\ttps = " + tps + ", sent to "
                                            + sendResult.getPartition() + "\t" + sendResult.getOffset());
                                    lastTime = System.currentTimeMillis();
                                }
                            }
                        }
                        catch (Exception e) {
                            System.out.println("sendMessage exception -------------");
                            e.printStackTrace();
                            try {
                                Thread.sleep(1000);
                            }
                            catch (InterruptedException e1) {
                            }
                        }
                    }
                }
            });
        }
    }


    private static MetaClientConfig buildMetaClientConfig() {
        MetaClientConfig config = new MetaClientConfig();
        return config;
    }


    private static Message buildMessage(String topic) {
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < MsgBodySize; i++) {
            sb.append("K");
        }
        Message m = new Message(topic, sb.toString().getBytes(), Long.toString(System.currentTimeMillis()));
        return m;
    }
}
