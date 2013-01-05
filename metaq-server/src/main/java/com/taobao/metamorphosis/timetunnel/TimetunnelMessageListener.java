//package com.taobao.metamorphosis.timetunnel;
//
//import java.io.IOException;
//import java.util.Iterator;
//import java.util.List;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.taobao.metamorphosis.network.PutCommand;
//import com.taobao.metamorphosis.server.CommandProcessor;
//import com.taobao.metamorphosis.utils.MetaMBeanServer;
//import com.taobao.timetunnel2.Message;
//import com.taobao.timetunnel2.SessionContext;
//import com.taobao.timetunnel2.Subscriber;
//
//
///**
// * @author 无花
// * @since 2011-6-7 下午12:03:41
// */
//
//public class TimetunnelMessageListener implements Subscriber, TimetunnelMessageListenerMBean {
//
//    static final Log log = LogFactory.getLog(TimetunnelMessageListener.class);
//
//    private final CommandProcessor commandProcessor;
//
//    private volatile boolean stop = false;
//
//
//    public TimetunnelMessageListener(final CommandProcessor commandProcessor) {
//        super();
//        this.commandProcessor = commandProcessor;
//        MetaMBeanServer.registMBean(this, null);
//    }
//
//
//    @Override
//    public void receive(final SessionContext sessionContext, final List<Message> messages) {
//        if (this.stop) {
//            return;
//        }
//        if (messages == null) {
//            log.warn("no message received form timetunnel");
//            return;
//        }
//
//        for (final Iterator<Message> iterator = messages.iterator(); iterator.hasNext();) {
//            final Message msg = iterator.next();
//            // String newTopic = "timetunnel-" + msg.getTopic();
//            if (msg.isCompressed()) {
//                msg.decompress();
//            }
//
//            try {
//                final byte[] data = getData(msg);
//                final PutCommand request = new PutCommand(msg.getTopic(), -1, data, null, 0, 0);
//                this.commandProcessor.processPutCommand(request, null, null);
//
//                // if (resp.getResponseStatus() != ResponseStatus.NO_ERROR) {
//                // // TODO tt没有重试机制？
//                // }
//            }
//            catch (final Throwable e) {
//                log.error("处理timetunnel消息失败.Append timetunnel message failed,topic=" + msg.getTopic(), e);
//                // 为了保险起见不抛出异常,可能会引起TT关闭连接等情况,跟TT开发人员确认中
//                // throw new
//                // RuntimeException("Append timetunnel message failed,topic=" +
//                // msg.getTopic(), e);
//            }
//
//        }
//
//    }
//
//
//    static byte[] getData(final Message msg) throws IOException {
//        return msg.getContent();// this.serializer.encodeObject(msg);
//    }
//
//
//    @Override
//    public void stopSub() {
//        this.stop = true;
//
//    }
//
//
//    @Override
//    public void startSub() {
//        this.stop = false;
//
//    }
//
//
//    @Override
//    public String getSubStatus() {
//        return this.stop ? "stoped" : "started";
//    }
//
//}
