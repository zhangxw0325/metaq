package com.taobao.metamorphosis.server.assembly;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 维护客户端的消息类型的关系 利用客户端的启动时间作为版本信息，当客户端应用配置的信息有多个版本的时候，以最后启动的信息为准
 * 
 * @author pingwei
 * 
 */
public class MessageTypeManager {
	
	static final Log log = LogFactory.getLog(MessageTypeManager.class);

	private final class MessageTypeSet {
		final Set<String> messageTypes;
		final long version;

		public MessageTypeSet(Set<String> messageTypes, long version) {
			this.messageTypes = messageTypes;
			this.version = version;
		}

		public Set<String> getMessageTypes() {
			return messageTypes;
		}

		public long getVersion() {
			return version;
		}

	}

	Object obj = new Object();

	ConcurrentHashMap<String/* group */, ConcurrentHashMap<String/* topic */, MessageTypeSet>> consumerTypeMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, MessageTypeSet>>();
	ConcurrentHashMap<String/*group topic*/, Set<Integer>> hashCodeMap = new ConcurrentHashMap<String, Set<Integer>>();
	
	/**
	 * 服务端对消息过滤，通过hashcode的方式比较，冲突的场景通过客户端解决
	 * @param group
	 * @param topic
	 * @return
	 */
	public Set<Integer> getMessageTypeHash(String group, String topic){
		MessageTypeSet value = getMessageType(group, topic);
		if (value == null) {
			return null;
		}
		StringBuilder keyBuilder = new StringBuilder(group.length() + 1 + topic.length());
		keyBuilder.append(group).append(" ").append(topic);
		String key = keyBuilder.toString();
		Set<Integer> hashList = this.hashCodeMap.get(key);
		if(hashList == null){
			hashList = new HashSet<Integer>();
			for(String type : value.getMessageTypes()){
				hashList.add(type.hashCode());
			}
			this.hashCodeMap.put(key, hashList);
		}
		return hashList;
	}

	public Set<String> getMessageType(String group, String topic, long version) {
		MessageTypeSet value = getMessageType(group, topic);
		if (value == null) {
			log.info("group["+group+"],topic["+topic+"]'s messageType has not found");
			return null;
		}
		if (value.getVersion() < version) {//如果版本号大于当前信息，需要客户端汇报新数据
			log.info("group["+group+"],topic["+topic+"]'s messageType out of date, need new data");
			return null;
		}
		return value.getMessageTypes();
	}

	/**
	 * 更新消息类型关系。执行更新操作在以下两种场景发生：1.请求的版本号大于当前的版本号；2.第一次请求
	 * 返回结果是更新之后最新的消息类型列表
	 * @param group
	 * @param topic
	 * @param messageTypes
	 * @param version
	 */
	public Set<String> updateMessageType(String group, String topic, Set<String> messageTypes, long version) {
		MessageTypeSet value = getMessageType(group, topic);
		if (value != null && value.getVersion() >= version) {// 如果请求的version版本比当前的小，忽略
			return value.getMessageTypes();
		}
		MessageTypeSet typeValue = new MessageTypeSet(messageTypes, version);
		ConcurrentHashMap<String, MessageTypeSet> topicMap = this.consumerTypeMap.get(group);
		if (topicMap == null) {
			ConcurrentHashMap<String, MessageTypeSet> tmp = new ConcurrentHashMap<String, MessageTypeManager.MessageTypeSet>();
			topicMap = this.consumerTypeMap.putIfAbsent(group, tmp);
			if (topicMap == null) {
				topicMap = tmp;
			}
		}
		synchronized (obj) {//有条件的更新，还是要额外加锁
			log.info("server received new messageType, group[" + group + "],topic[" + topic + "], content["
					+ messageTypes +"]");
			MessageTypeSet oldValue = topicMap.putIfAbsent(topic, typeValue);
			if (oldValue != null && oldValue.getVersion() < version) {
				topicMap.put(topic, typeValue);
			}
		}
		//如果有修改删除hashCodeMap的数据
		StringBuilder keyBuilder = new StringBuilder(1 + group.length() + topic.length());
		keyBuilder.append(group).append(" ").append(topic);
		this.hashCodeMap.remove(keyBuilder.toString());
		return getMessageType(group, topic).getMessageTypes();
	}

	private MessageTypeSet getMessageType(String group, String topic) {
		ConcurrentHashMap<String, MessageTypeSet> topicMap = this.consumerTypeMap.get(group);
		if (topicMap == null) {
			return null;
		}
		MessageTypeSet value = topicMap.get(topic);
		return value;
	}
}
