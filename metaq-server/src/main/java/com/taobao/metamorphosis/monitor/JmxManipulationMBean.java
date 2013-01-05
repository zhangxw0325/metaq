package com.taobao.metamorphosis.monitor;

public interface JmxManipulationMBean {

	/**
	 * 触发删除物理文件和逻辑文件
	 */
	public void triggerDeleteFiles();
	
}
