package com.taobao.metamorphosis.network;

import com.taobao.gecko.core.buffer.IoBuffer;


/**
 * 协议编码接口和常量
 * 
 * @author boyan
 * @Date 2011-6-2
 * 
 */
public interface MetaEncodeCommand {
    /**
     * 编码协议
     * 
     * @return 编码后的buffer
     */
    public IoBuffer encode();

    byte SPACE = (byte) ' ';
    byte[] CRLF = { '\r', '\n' };
    public String GET_CMD = "get";
    public String RESULT_CMD = "result";
    public String OFFSET_CMD = "offset";
    public String PUT_CMD = "put";
    public String SYNC_CMD = "sync";
    public String QUIT_CMD = "quit";
    public String VERSION_CMD = "version";
    public String STATS_CMD = "stats";
    public String TRANS_CMD = "transaction";
    public String ASK_CMD = "ask";
    public String FETCH_CMD = "fetch";
    public String MESSAGETYPE_CMD = "messageType";
    
}
