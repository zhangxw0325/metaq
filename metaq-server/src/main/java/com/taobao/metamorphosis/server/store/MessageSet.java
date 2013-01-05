package com.taobao.metamorphosis.server.store;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.server.network.SessionContext;


/**
 * 消息集合
 * 
 * @author boyan
 * @Date 2011-4-19
 * 
 */
public interface MessageSet {

    public MessageSet slice(long offset, long limit) throws IOException;


    public void write(GetCommand getCommand, SessionContext ctx);


    public long append(ByteBuffer buff) throws IOException;


    public void flush() throws IOException;


    public void read(final ByteBuffer bf, long offset) throws IOException;


    public void read(final ByteBuffer bf) throws IOException;


    public long getMessageCount();

}
