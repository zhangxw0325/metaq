/**
 * $Id: DequeueResult.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.client.consumer;

import java.util.List;

import com.taobao.metamorphosis.Message;


public class DequeueResult {
    private DequeueStatus status;
    private List<Message> msgList;
    private long movedOffset = 0;
    private long lastMsgOffset = 0;


    public DequeueResult(DequeueStatus status, List<Message> msgList, long movedOffset) {
        this.status = status;
        this.msgList = msgList;
        this.movedOffset = movedOffset;
        if (msgList != null && !msgList.isEmpty()) {
            lastMsgOffset = msgList.get(msgList.size() - 1).getOffset();
        }
    }


    public DequeueStatus getStatus() {
        return status;
    }


    public void setStatus(DequeueStatus status) {
        this.status = status;
    }


    public List<Message> getMsgList() {
        return msgList;
    }


    public void setMsgList(List<Message> msgList) {
        this.msgList = msgList;
    }


    public long getMovedOffset() {
        return movedOffset;
    }


    public void setMovedOffset(long movedOffset) {
        this.movedOffset = movedOffset;
    }


    public long getLastMsgOffset() {
        return lastMsgOffset;
    }


    public void setLastMsgOffset(long lastMsgOffset) {
        this.lastMsgOffset = lastMsgOffset;
    }
}
