/**
 * $Id: MetaStoreConfigMBean.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

/**
 * ¥Ê¥¢≤„≈‰÷√Bean
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface MetaStoreConfigMBean {
	
	/**
	 * reload metastore configuration
	 */
	public void reload(String configPath);
	
}
