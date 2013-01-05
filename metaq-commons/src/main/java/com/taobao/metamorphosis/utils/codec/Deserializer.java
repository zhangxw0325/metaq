package com.taobao.metamorphosis.utils.codec;

import java.io.IOException;

/**
 * 
 * @author wuxin
 * @since 1.0, 2009-10-20 上午09:42:35
 */
public interface Deserializer {
	/**
	 * 将指定的字节码反序列化.
	 * 
	 * @param in - 指定的字节码内容
	 * @return   - 返回反序列化后的对象
	 */
	public Object decodeObject(byte[] in)throws IOException;
}
