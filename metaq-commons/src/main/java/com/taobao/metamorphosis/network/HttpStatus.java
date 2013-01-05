package com.taobao.metamorphosis.network;

/**
 * 响应状态码，遵循http语义
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class HttpStatus {
    public static final int BadRequest = 400;
    public static final int NotFound = 404;
    public static final int Forbidden = 403;
    public static final int Unauthorized = 401;

    public static final int InternalServerError = 500;
    public static final int ServiceUnavilable = 503;
    public static final int GatewayTimeout = 504;

    public static final int Success = 200;

    public static final int Moved = 301;
    
    public static final int Continue= 100;//请求不完整，metaq2.0的协议支持消息类型过滤，但是需要客户端汇报需要的数据，如果没有汇报就拉取数据会出现这种情况
}
