package com.taobao.metamorphosis.tools.monitor.msgprobe;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tools.monitor.alert.Alarm;
import com.taobao.metamorphosis.tools.monitor.core.MonitorConfig;
import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ReveiceResult;
import com.taobao.metamorphosis.tools.monitor.core.SendResultWrapper;
import com.taobao.metamorphosis.tools.monitor.msgprobe.MsgProber.ProbContext;
import com.taobao.notify.msgcenter.PushMsg;

/**
 * @author 无花
 * @since 2011-5-25 上午11:25:59
 */
//note:发旺旺消息字数有限制
public class AlarmProbeListener extends ProbeListener {

    private MonitorConfig monitorConfig;
    static final private String wwTitle = "metamorphosis monitor alert";

    public AlarmProbeListener(MonitorConfig monitorConfig) {
        this.monitorConfig = monitorConfig;
        PushMsg.setWangwangTitle(wwTitle);
    }

    @Override
    protected void onReceiveFail(ProbContext probContext) {
        ReveiceResult revResult = probContext.getReveiceResult();
        String exception = revResult.getException() != null ? revResult.getException().getMessage() : StringUtils.EMPTY;
        String msg = String
                .format(
                        "fail on receive message from[%s].\n %s second passed since last send msg;\n topic[%s];\n partition[%s];\n offset[%s];\n exception[%s]",
                        revResult.getServerUrl(), probContext.getSendRevInterval(), revResult.getTopic(), revResult
                                .getPartition(), revResult.getOffset(), exception);
        getLogger().warn("alarm...");
        Alarm.alert(msg, monitorConfig);
    }

    @Override
    protected void onSendFail(MsgSender sender, SendResultWrapper result) {

        String exception = result.getException() != null ? result.getException().getMessage() : StringUtils.EMPTY;
        String msg = String.format("fail send message to %s ; errorMsg[%s];\n exception[%s]", sender.getServerUrl(),
                result.getErrorMessage(), exception);

        getLogger().warn("alarm...");
        Alarm.alert(msg, monitorConfig);
    }

}
