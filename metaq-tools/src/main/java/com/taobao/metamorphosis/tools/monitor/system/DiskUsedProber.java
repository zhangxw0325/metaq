package com.taobao.metamorphosis.tools.monitor.system;

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.utils.DiskUsedUtil;
import com.taobao.metamorphosis.tools.utils.MonitorResult;


/**
 * 
 * @author 无花
 * @since 2011-9-28 下午3:19:56
 */

public class DiskUsedProber extends SystemProber {

    public DiskUsedProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected MonitorResult getMonitorResult(MsgSender sender) {
        return DiskUsedUtil.getDiskUsed(sender.getHost(), this.getMonitorConfig().getLoginUser(), this
            .getMonitorConfig().getLoginPassword());
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {
        if (monitorResult.getValue().intValue() > this.getMonitorConfig().getDiskUsedThreshold()) {
            this.alert(monitorResult.getIp() + "磁盘使用已经到达百分之 " + monitorResult.getValue());
        }

    }

}
