package com.taobao.metamorphosis.network;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.taobao.gecko.core.buffer.IoBuffer;

/**
 * messageType version group topic opaque clientStartTime dataLength\r\n
 * messageTypes
 * 
 * @author pingwei
 * 
 */
public class MessageTypeCommand extends AbstractRequestCommand {
	static final long serialVersionUID = -1L;
	public static final char SEP = (char) 04;
	private final String version;
	private final Set<String> messageTypes;
	private final String group;
	private final long clientStartTime;

	public MessageTypeCommand(String version, String group, String topic, int opaque, Set<String> messageTypes,
			long clientStartTime) {
		super(topic, opaque);
		this.version = version;
		this.group = group;
		this.messageTypes = new HashSet<String>();
		this.clientStartTime = clientStartTime;
		checkMessageTypes(messageTypes);
	}

	public long getClientStartTime() {
		return clientStartTime;
	}

	/**
	 * 过滤掉一些不合法的type，以免引起不必要的消耗
	 */
	private void checkMessageTypes(Set<String> messageTypes) {
		if (messageTypes == null || messageTypes.isEmpty()) {
			this.messageTypes.add("*");
			return;
		}
		Iterator<String> it = messageTypes.iterator();
		while (it.hasNext()) {
			String type = it.next();
			if (!StringUtils.isBlank(type)) {
				this.messageTypes.add(type);
			}
		}
	}

	public String getVersion() {
		return version;
	}

	public String getGroup() {
		return group;
	}

	public Set<String> getMessageTypes() {
		return messageTypes;
	}

	// messageType version group topic opaque clientStartTime dataLength\r\n
	// messageTypes
	@Override
	public IoBuffer encode() {
		int dataLen = calcMessageTypesLength();
		IoBuffer buf = IoBuffer.allocate(19 + getVersion().length() + getGroup().length() + getTopic().length()
				+ ByteUtils.stringSize(getOpaque()) + ByteUtils.stringSize(dataLen)
				+ ByteUtils.stringSize(getClientStartTime()) + dataLen);
		ByteUtils.setArguments(buf, MetaEncodeCommand.MESSAGETYPE_CMD, getVersion(), getGroup(), getTopic(),
				getOpaque(), getClientStartTime(), dataLen);
		buf.put(joinMessageTypes().getBytes());
		buf.flip();
		return buf;
	}

	private String joinMessageTypes() {
		if (this.messageTypes == null || this.messageTypes.isEmpty()) {
			return "*";
		}
		return StringUtils.join(this.messageTypes, SEP);
	}

	private int calcMessageTypesLength() {
		if (this.messageTypes == null || this.messageTypes.isEmpty()) {// 如果是空，默认为*，表示不过滤
			return 1;
		}
		int len = 0;
		for (String type : this.messageTypes) {
			if (StringUtils.isBlank(type)) {
				continue;
			}
			len += (type.length() + 1);// 除了type的长度，还需要加上分隔符的长度
		}
		return --len;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (clientStartTime ^ (clientStartTime >>> 32));
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + ((messageTypes == null) ? 0 : messageTypes.hashCode());
		result = prime * result + ((version == null) ? 0 : version.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageTypeCommand other = (MessageTypeCommand) obj;
		if (clientStartTime != other.clientStartTime)
			return false;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (messageTypes == null) {
			if (other.messageTypes != null)
				return false;
		} else if (!messageTypes.equals(other.messageTypes))
			return false;
		if (version == null) {
			if (other.version != null)
				return false;
		} else if (!version.equals(other.version))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MessageTypeCommand [version=" + version + ", messageTypes=" + messageTypes + ", group=" + group
				+ ", clientStartTime=" + clientStartTime + "]";
	}
	
	

}
