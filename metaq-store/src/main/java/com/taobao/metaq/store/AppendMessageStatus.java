/**
 * $Id: AppendMessageStatus.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

/**
 * 写入消息状态
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public enum AppendMessageStatus {
    // 成功追加消息
    PUT_OK,
    // 走到文件末尾
    END_OF_FILE,
    // 消息大小超限
    MESSAGE_SIZE_EXCEEDED,
    // 未知错误
    UNKNOWN_ERROR,
}
