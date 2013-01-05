package com.taobao.metamorphosis.network;

import com.taobao.gecko.core.buffer.IoBuffer;


/**
 * 退出命令，客户端发送此命令后，服务器将主动关闭连接
 * 
 * @author boyan
 * @Date 2011-4-22
 * 
 */
public class QuitCommand extends AbstractRequestCommand {

    static final long serialVersionUID = -1L;


    public QuitCommand() {
        super(null, Integer.MAX_VALUE);
    }

    static final IoBuffer QUIT_BUF = IoBuffer.wrap((MetaEncodeCommand.QUIT_CMD + "\r\n").getBytes());


    @Override
    public IoBuffer encode() {
        return QUIT_BUF.slice();
    }

}
