package com.taobao.metamorphosis.tail4j;

import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.tail4j.Config.LogConfig;


/**
 * 消息发送者
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class Sender {

    private static final String DEFAULT_ENCODING = "utf-8";

    private final LogConfig logConfig;

    private final MessageProducer messageProducer;

    static final Log log = LogFactory.getLog(Sender.class);


    public Sender(LogConfig logConfig, MessageProducer messageProducer) {
        super();
        this.logConfig = logConfig;
        this.messageProducer = messageProducer;
        this.messageProducer.publish(this.logConfig.topic);
    }


    public void close() {
        try {
            this.messageProducer.shutdown();
        }
        catch (MetaClientException e) {
            // ignore
        }
    }


    public boolean send(byte[] data, String currentLogPath) throws InterruptedException {
        // 如果没有主动设置utf-8,首先判断是否已经是utf-8编码
        if (!DEFAULT_ENCODING.equalsIgnoreCase(this.logConfig.encoding)) {
            // 如果不是utf-8，转码成utf-8
            if (!UTF8.isValidUtf8(data)) {
                try {
                    data = new String(data).getBytes(DEFAULT_ENCODING);
                }
                catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        try {
            SendResult sendResult =
                    this.messageProducer.sendMessage(new Message(this.logConfig.topic, data, currentLogPath));
            if (!sendResult.isSuccess()) {
                log.error("Send message failed,error message:" + sendResult.getErrorMessage());
            }
            return sendResult.isSuccess();
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (MetaClientException e) {
            throw new RuntimeException(e);
        }
    }
}
