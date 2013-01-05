package com.taobao.metamorphosis.network;

import com.taobao.gecko.core.buffer.IoBuffer;


/**
 * 同步复制，master/slave复制消息的协议,协议格式如下：</br> sync topic partition value-length flag
 * msgId opaque\r\ndata
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public class SyncCommand extends PutCommand {
    private final long msgId;


    public SyncCommand(final String topic, final int partition, final byte[] data, final long msgId, final int flag,
            final Integer opaque) {
        super(topic, partition, data, null, flag, opaque);
        this.msgId = msgId;
    }


    public long getMsgId() {
        return this.msgId;
    }


    @Override
    public IoBuffer encode() {
        final int dataLen = this.data == null ? 0 : this.data.length;
        final IoBuffer buffer =
                IoBuffer.allocate(12 + ByteUtils.stringSize(this.partition) + ByteUtils.stringSize(dataLen)
                        + ByteUtils.stringSize(this.getOpaque()) + this.getTopic().length()
                        + ByteUtils.stringSize(this.msgId) + ByteUtils.stringSize(this.flag) + dataLen);

        ByteUtils.setArguments(buffer, MetaEncodeCommand.SYNC_CMD, this.getTopic(), this.partition, dataLen, this.flag,
            this.msgId, this.getOpaque());

        if (this.data != null) {
            buffer.put(this.data);
        }
        buffer.flip();
        return buffer;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) (this.msgId ^ this.msgId >>> 32);
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final SyncCommand other = (SyncCommand) obj;
        if (this.msgId != other.msgId) {
            return false;
        }
        return true;
    }

}
