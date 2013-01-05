package com.taobao.metamorphosis.tools.monitor.system;

import com.taobao.metamorphosis.tools.monitor.core.CoreManager;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.utils.MonitorResult;
import com.taobao.metamorphosis.tools.utils.TransactionUtil;


/**
 * 监控事务挂起的数量
 * 
 * @author 无花
 * @since 2011-9-30 上午10:30:35
 */

public class PreparedTransactionProber extends SystemProber {

    public PreparedTransactionProber(CoreManager coreManager) {
        super(coreManager);
    }


    @Override
    protected MonitorResult getMonitorResult(MsgSender sender) throws Exception {
        int preparedTransactionCount =
                TransactionUtil.getPreparedTransactionCount(sender.getHost(), this.getMonitorConfig().getJmxPort());
        String msg = sender.getServerUrl() + " 事务挂起的数量达到" + preparedTransactionCount;
        this.logger.debug(msg);
        if (preparedTransactionCount >= this.getMonitorConfig().getPreparedTransactionCountThreshold()) {
            this.alert(msg);
        }
        return null;
    }


    @Override
    protected void processResult(MonitorResult monitorResult) {

    }


    public static void main(String[] args) throws Exception {
        int preparedTransactionCount = TransactionUtil.getPreparedTransactionCount("10.232.102.184", 9999);
        System.out.println(preparedTransactionCount);
    }

}
