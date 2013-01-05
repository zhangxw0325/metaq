package com.taobao.metamorphosis.utils.codec;

import java.io.IOException;

/**
 * 
 * @author wuxin
 * @since 1.0, 2009-10-20 上午09:41:40
 */
public interface Serializer {
	/**
	 * 将指定的对象进行序列化.
	 * 
	 * @param obj - 需要序列化的对象
	 * @return    - 返回对象序列化后的字节码
	 */
	public byte[] encodeObject(Object obj)throws IOException;
}
