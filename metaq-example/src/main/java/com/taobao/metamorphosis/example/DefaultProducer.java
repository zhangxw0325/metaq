/**
 * $Id: DefaultProducer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.example;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;


public class DefaultProducer {
    public static void main(final String[] args) throws Exception {
        // New session factory
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(buildMetaClientConfig());
        // create producer
        final MessageProducer producer = sessionFactory.createProducer();
        // publish topic
        final String topic = "meta-test-20";
        // producer.setDefaultTopic("notify2meta_default");
        producer.publish(topic);

        long lastTime = System.currentTimeMillis();

        for (long i = 1;; i++) {
            try {
                final SendResult sendResult = producer.sendMessage(buildMessage(topic, (int) i));
                if (!sendResult.isSuccess()) {
                    System.err.println(i + "\tSend message failed,error message:" + sendResult.getErrorMessage());
                }
                else {

                    if ((i % 10) == 0) {
                        long eclipseTime = System.currentTimeMillis() - lastTime;
                        double tps = 10 / (eclipseTime * 0.001);
                        System.out.println(i + "\ttps = " + tps + ", sent to " + sendResult.getPartition() + "\t"
                                + sendResult.getOffset());
                        lastTime = System.currentTimeMillis();

                    }
                }
            }
            catch (Exception e) {
                Thread.sleep(1000 * 3);
                e.printStackTrace();
            }
        }
    }


    private static MetaClientConfig buildMetaClientConfig() {
        MetaClientConfig config = new MetaClientConfig();
        // config.setDiamondZKDataId("meta.debug.zkConfig");
        return config;
    }


    private static Message buildMessage(String topic, int index) {
        return new Message(topic, ("hi,nice,aaaaaaaaaaaaaaaaaaaaaaaaaaaaa#" + index).getBytes(),
            Long.toString(System.currentTimeMillis()));
    }
}
