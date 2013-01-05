package com.taobao.metamorphosis.tools.monitor.msgprobe;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.SendResultWrapper;
import com.taobao.metamorphosis.tools.monitor.msgprobe.MsgProber.ProbContext;

/**
 * @author 无花
 * @since 2011-5-25 上午11:53:10
 */

abstract public class ProbeListener {
    private static Logger listenerLogger = Logger.getLogger("probeListener");

    /**发送消息失败时**/
    abstract protected void onSendFail(MsgSender sender, SendResultWrapper result);

    /**接收消息失败时**/
    abstract protected void onReceiveFail(ProbContext probContext);

    protected Logger getLogger() {
        return listenerLogger;
    }

}
