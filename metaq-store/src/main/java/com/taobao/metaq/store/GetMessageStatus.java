/**
 * $Id: GetMessageStatus.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

/**
 * 拉消息状态码
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public enum GetMessageStatus {
    // 找到消息
    FOUND,
    // offset正确，但是过滤后没有匹配的消息
    NO_MATCHED_MESSAGE,
    // offset正确，但是物理队列消息正在被删除
    MESSAGE_WAS_REMOVING,
    // offset正确，但是从逻辑队列没有找到，可能正在被删除
    OFFSET_FOUND_NULL,
    // offset错误，严重溢出
    OFFSET_OVERFLOW_BADLY,
    // offset错误，溢出1个
    OFFSET_OVERFLOW_ONE,
    // offset错误，太小了
    OFFSET_TOO_SMALL,
    // 没有对应的逻辑队列
    NO_MATCHED_LOGIC_QUEUE,
    // 队列中一条消息都没有
    NO_MESSAGE_IN_QUEUE,
}
