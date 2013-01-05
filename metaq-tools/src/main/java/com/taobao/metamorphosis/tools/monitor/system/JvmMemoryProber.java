package com.taobao.metamorphosis.tools.monitor.system;

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.utils.JvmMemoryUtil;
import com.taobao.metamorphosis.tools.utils.MonitorResult;


/**
 * 
 * @author 无花
 * @since 2011-9-28 下午5:33:00
 */

public class JvmMemoryProber extends SystemProber {

    public JvmMemoryProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected MonitorResult getMonitorResult(MsgSender sender) {
        try {
            return JvmMemoryUtil.getMemoryInfo(sender.getHost(), this.getMonitorConfig().getJmxPort());
        }
        catch (Throwable e) {
            this.logger.error(e);
            return null;
        }
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {
        if (monitorResult.getValue() > 80) {
            this.alert(monitorResult.getIp() + "JVM 内存使用已经到达百分之 " + monitorResult.getValue());
        }
    }


    public static void main(String[] args) throws Exception {
        MonitorResult result = JvmMemoryUtil.getMemoryInfo("10.232.102.184", 9999);
        System.out.println(result.getValue());
    }

}
