package com.taobao.metamorphosis.tools.monitor.core;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class MonitorConfigTest {
	
	@Test
	public void testLoadInit() throws IOException{
		String resource = "D:/meta/metamorphosis-ops/src/main/webconfig/monitor.ini";
		MonitorConfig monitorConfig = new MonitorConfig();
		monitorConfig.loadInis(resource);
		Assert.assertTrue(monitorConfig.getWangwangList().contains("ÎÞ»¨"));
		
	}
}
