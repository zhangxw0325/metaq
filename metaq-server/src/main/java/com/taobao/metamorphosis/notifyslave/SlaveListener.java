package com.taobao.metamorphosis.notifyslave;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.AppendMessageErrorException;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.notify.codec.Serializer;
import com.taobao.notify.codec.impl.JavaSerializer;
import com.taobao.notify.message.Message;
import com.taobao.notify.remotingclient.MessageListener;
import com.taobao.notify.remotingclient.MessageStatus;


/**
 * notify消息监听器，收到消息append到meta
 * 
 * @author boyan
 * @Date 2011-5-12
 */
public class SlaveListener implements MessageListener {
    private final CommandProcessor commandProcessor;

    private final Serializer serializer;
    static final Log log = LogFactory.getLog(SlaveListener.class);


    public SlaveListener(final CommandProcessor commandProcessor) {
        super();
        this.commandProcessor = commandProcessor;
        this.serializer = new JavaSerializer();
    }

    static final class PutOp implements PutCallback {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile com.taobao.gecko.core.command.ResponseStatus status;


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (this.status == null ? 0 : this.status.hashCode());
            return result;
        }


        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (this.getClass() != obj.getClass()) {
                return false;
            }
            final PutOp other = (PutOp) obj;
            if (this.status != other.status) {
                return false;
            }
            return true;
        }


        @Override
        public void putComplete(final com.taobao.gecko.core.command.ResponseCommand resp) {
            this.status = resp.getResponseStatus();
            this.latch.countDown();
        }

    }

    static final long PUT_TIMEOUT = 5000L;


    @Override
    public void receiveMessage(final Message msg, final MessageStatus status) {
        final String newTopic = msg.getTopic() + "-" + msg.getMessageType();
        try {
            final byte[] data = this.getData(msg);
            final PutCommand putCommand = new PutCommand(newTopic, -1, data, null, 0, 0);

            final PutOp cb = new PutOp();
            this.commandProcessor.processPutCommand(putCommand, null, cb);
            cb.latch.await(PUT_TIMEOUT, TimeUnit.MILLISECONDS);
            if (cb.status != com.taobao.gecko.core.command.ResponseStatus.NO_ERROR) {
                status.setRollbackOnly();
            }
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (final Throwable e) {
            log.error("处理notify消息失败", e);
            throw new AppendMessageErrorException("Append message failed,topic=" + newTopic, e);
        }
    }


    private byte[] getData(final Message msg) throws IOException {
        return this.serializer.encodeObject(msg);
    }
}
