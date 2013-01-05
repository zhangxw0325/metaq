package com.taobao.metamorphosis.network;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.command.kernel.HeartBeatRequestCommand;


/**
 * 查询服务器版本，也用于心跳检测，协议：version opaque\r\n
 * 
 * @author boyan
 * @Date 2011-4-22
 * 
 */
public class VersionCommand extends AbstractRequestCommand implements HeartBeatRequestCommand {
    static final long serialVersionUID = -1L;


    public VersionCommand(final Integer opaque) {
        super(null, opaque);
    }


    @Override
    public IoBuffer encode() {
        return IoBuffer.wrap((MetaEncodeCommand.VERSION_CMD + " " + this.getOpaque() + "\r\n").getBytes());
    }

}
