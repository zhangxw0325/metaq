package com.taobao.metamorphosis.tools.monitor.msgprobe;

import com.taobao.metamorphosis.tools.monitor.core.MsgSender;
import com.taobao.metamorphosis.tools.monitor.core.ReveiceResult;
import com.taobao.metamorphosis.tools.monitor.core.SendResultWrapper;
import com.taobao.metamorphosis.tools.monitor.msgprobe.MsgProber.ProbContext;

/**
 * @author ÎÞ»¨
 * @since 2011-5-25 ÉÏÎç11:20:50
 */

public class DefaultProbeListener extends ProbeListener {
    
    @Override
    protected void onReceiveFail(ProbContext probContext) {
        ReveiceResult revResult = probContext.getReveiceResult();
        getLogger()
                .warn(
                        String
                                .format(
                                        "fail on receive message from[%s].\n %s second passed since last send msg;\n topic[%s];\n partition[%s];\n offset[%s]",
                                        revResult.getServerUrl(), probContext.getSendRevInterval(), revResult
                                                .getTopic(), revResult.getPartition(), revResult.getOffset()),
                        probContext.getReveiceResult().getException());
    }

    @Override
    protected void onSendFail(MsgSender sender, SendResultWrapper result) {
        getLogger().warn(
                String.format("fail send message to %s ;error[%s];", sender.getServerUrl(), result.getErrorMessage()),
                result.getException());
    }

}
