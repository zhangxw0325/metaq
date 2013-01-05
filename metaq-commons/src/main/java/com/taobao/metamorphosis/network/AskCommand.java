package com.taobao.metamorphosis.network;

import java.util.ArrayList;
import java.util.List;

import com.taobao.gecko.core.buffer.IoBuffer;

/**
 * topic条件查询 格式：</br> ask topic type params\r\n </br>params 是可选的
 */
public class AskCommand extends AbstractRequestCommand {
	static final long serialVersionUID = -1L;
	
	// 请求操作类型
	private final String type;
	
	private final String[] params;
	
	
	public AskCommand(final String topic, String type, String[] params) {
		super(topic, Integer.MAX_VALUE);
		this.type=type;
		this.params=params;
	}
	

	public String getType() {
		return type;
	}


	public String[] getParams() {
		return params;
	}


	@Override
	public IoBuffer encode() {
		List<String> list=new ArrayList<String>();
		list.add(ASK_CMD);
		list.add(this.getTopic());
		list.add(type);

		int totalSize=ASK_CMD.length() + 1 + this.getTopic().length() + 1 + type.length();
		for(String p : params) {
			totalSize += 1+p.length();
			list.add(p);
		}
		totalSize += 2;
		final IoBuffer buf = IoBuffer.allocate(totalSize);
		ByteUtils.setArguments(buf, list.toArray());
		buf.flip();
		return buf;
	}
}
