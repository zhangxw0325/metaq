/**
 * $Id: MessageListListener.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.client.consumer;

import java.util.List;

import com.taobao.metamorphosis.Message;


public interface MessageListListener extends MessageListener {
    public void recieveMessageList(List<Message> msgs);
}
