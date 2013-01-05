package com.taobao.metamorphosis.network;

import com.taobao.gecko.core.buffer.IoBuffer;

/**
 * metaq2.0的获取消息协议。相比GetCommand，支持服务端消息过滤; 
 * 协议格式：fetch version topic group partition offset maxSize opaque clientStartTime\r\n;
 * 
 * @author pingwei
 * 
 */
public class FetchCommand extends GetCommand {

	static final long serialVersionUID = -1L;
	static final String SEPARATOR = Character.toString((char) 0x1);

	private final String version;
	private final long clientStartTime;

	public FetchCommand(String version, String topic, String group, int partition, long offset, int maxSize,
			Integer opaque, long clientStartTime) {
		super(topic, group, partition, offset, maxSize, opaque);
		this.version = version;
		this.clientStartTime = clientStartTime;
	}

	public String getVersion() {
		return version;
	}

	public long getClientStartTime() {
		return clientStartTime;
	}

	// fetch version topic group partition offset maxSize opaque clientStartTime\r\n
	@Override
	public IoBuffer encode() {
		final IoBuffer buffer = IoBuffer.allocate(15 + getVersion().length() + getTopic().length()
				+ this.getGroup().length() + ByteUtils.stringSize(getPartition())
				+ ByteUtils.stringSize(getOpaque()) + ByteUtils.stringSize(getOffset())
				+ ByteUtils.stringSize(getMaxSize()) + ByteUtils.stringSize(getClientStartTime()));
		ByteUtils.setArguments(buffer, MetaEncodeCommand.FETCH_CMD, this.getVersion(), this.getTopic(),
				this.getGroup(), getPartition(), getOffset(), getMaxSize(), this.getOpaque(), this.getClientStartTime());
		buffer.flip();
		return buffer;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
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
		FetchCommand other = (FetchCommand) obj;
		if (version == null) {
			if (other.version != null)
				return false;
		} else if (!version.equals(other.version))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FetchCommand [version=" + version + ", clientStartTime=" + clientStartTime + "]";
	}

	

}
