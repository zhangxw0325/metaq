package com.taobao.metamorphosis.notifyslave;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.alibaba.common.lang.StringUtil;
import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.notify.remotingclient.DefaultNotifyManager;
import com.taobao.notify.remotingclient.MessageListener;
import com.taobao.notify.remotingclient.NotifyManager;
import com.taobao.notify.subscription.Binding;


/**
 * 作为notify slave的meta broker plugin
 * 
 * @author boyan
 * @Date 2011-5-12
 * @author 无花
 * @Date 2011-6-09
 */
public class SlaveBroker extends AbstractBrokerPlugin {

    private NotifyManager notifyManager;
    private MessageListener messageListener;
    private List<NotifySubInfo> notifySubInfos;
    private String groupId;


    @Override
    public void start() {
        this.subscribeNotify();
    }


    @Override
    public void stop() {
        this.notifyManager.close();
    }


    // public SlaveBroker(MetaMorphosisBroker broker, Properties props) {
    // // this.broker = broker;//new MetaMorphosisBroker(metaConfig);
    // // this.props = props;
    // super(broker, props);
    // }

    @Override
    public void init(final MetaMorphosisBroker metaMorphosisBroker, final Properties props) {
        this.broker = metaMorphosisBroker;
        this.props = props;
        this.messageListener = new SlaveListener(this.broker.getBrokerProcessor());
        this.groupId = props.getProperty("notify-groupId");
        if (StringUtil.isBlank(this.groupId)) {
            throw new MetamorphosisServerStartupException("Blank notify groupId");
        }
        this.notifyManager =
                new DefaultNotifyManager(this.groupId, props.getProperty("notify-name"), "metamorhposis notify slave",
                    this.messageListener);
        final String topicsStr = props.getProperty("notify-topics");
        this.notifySubInfos = this.parseSubInfo(topicsStr);
        if (this.notifySubInfos == null || this.notifySubInfos.isEmpty()) {
            throw new MetamorphosisServerStartupException("Empty topics");
        }
    }


    private List<NotifySubInfo> parseSubInfo(final String topicsStr) {
        // topic1:tpye1,type2;topic2:tpye1,type2...;topic3

        final List<NotifySubInfo> list = new ArrayList<SlaveBroker.NotifySubInfo>();
        if (StringUtils.isBlank(topicsStr)) {
            return list;
        }

        final String[] topicTypes = StringUtils.splitByWholeSeparator(topicsStr, ";");
        for (final String topicType : topicTypes) {
            list.add(new NotifySubInfo(topicType));
        }
        return list;
    }


    @Override
    public String name() {
        return "notify";
    }


    private void subscribeNotify() {
        for (final NotifySubInfo notifySubInfo : this.notifySubInfos) {
            if (notifySubInfo.hasMessageTypes()) {
                for (final String messageType : notifySubInfo.messageTypes) {
                    log.info("订阅Notify topic=" + notifySubInfo.topic + "messageType=" + messageType + "...");
                    this.notifyManager.subscribe(Binding.direct(notifySubInfo.topic, messageType, this.groupId, 40000,
                        true));
                }
            }
            else {
                log.info("订阅Notify topic=" + notifySubInfo.topic + "...");
                this.notifyManager.subscribe(Binding.fanout(notifySubInfo.topic, this.groupId, 40000, true));
            }

        }
    }

    private static class NotifySubInfo {
        String topic;
        Set<String> messageTypes;


        public NotifySubInfo(final String topicType) {
            final int index = topicType.indexOf(":");
            if (index != -1) {
                this.topic = topicType.substring(0, index);
                final String[] types = topicType.substring(index + 1).split(",");
                if (types != null && types.length > 0) {
                    for (final String type : types) {
                        this.addMessageType(type);
                    }
                }
            }
            else {
                this.topic = topicType;
            }
        }


        private void addMessageType(final String messageType) {
            if (messageTypes == null) {
                messageTypes = new HashSet<String>();
            }
            messageTypes.add(messageType);
        }


        boolean hasMessageTypes() {
            return messageTypes != null && messageTypes.size() > 0;
        }
    }

}
