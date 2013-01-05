/**
 * $Id: FetchResult.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.client.consumer;

import java.util.List;

import com.taobao.metamorphosis.Message;


public class FetchResult {
    private final boolean newMetaServer;
    private List<Message> messageList;
    private MessageIterator messageIterator;


    public FetchResult(boolean newMetaServer, List<Message> messageList, MessageIterator messageIterator) {
        this.newMetaServer = newMetaServer;
        this.messageList = messageList;
        this.messageIterator = messageIterator;
    }


    public List<Message> getMessageList() {
        return messageList;
    }


    public void setMessageList(List<Message> messageList) {
        this.messageList = messageList;
    }


    public MessageIterator getMessageIterator() {
        return messageIterator;
    }


    public void setMessageIterator(MessageIterator messageIterator) {
        this.messageIterator = messageIterator;
    }


    public boolean isNewMetaServer() {
        return newMetaServer;
    }
}
