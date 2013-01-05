package com.taobao.metamorphosis.client.extension;

import com.taobao.metamorphosis.client.MessageSessionFactory;


/**
 * <pre>
 * 消息会话工厂，meta客户端的主接口,推荐一个应用只使用一个.
 * 需要按照消息内容(例如某个id)散列到固定分区并要求有序的场景中使用.
 * </pre>
 * 
 * @author 无花
 * @since 2011-8-24 下午4:30:36
 */

public interface OrderedMessageSessionFactory extends MessageSessionFactory {

}
