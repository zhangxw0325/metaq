/**
 * $Id: MetaStore.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

import java.util.Set;

import com.taobao.metaq.commons.MetaMessage;
import com.taobao.metaq.commons.MetaMessageAnnotation;
import com.taobao.metaq.commons.MetaMessageWrapper;


/**
 * 存储层接口
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface MetaStore {
    public static final String MetaStoreLogName = "MetaStore";


    /**
     * 重启时，加载数据
     */
    public boolean load();


    /**
     * 启动服务
     */
    public void start() throws Exception;


    /**
     * 关闭服务
     */
    public void shutdown();


    /**
     * 删除所有文件，单元测试会使用
     */
    public void destroy();


    /**
     * 存储消息
     */
    public PutMessageResult putMessage(final MetaMessage msg, final MetaMessageAnnotation msgant);


    /**
     * 读取消息，如果types为null，则不做过滤
     */
    public GetMessageResult getMessage(final String topic, final int queueId, final long offset,
            final int maxSize, final Set<Integer> types);


    /**
     * 获取指定队列最大Offset 如果队列不存在，返回-1
     */
    public long getMaxOffsetInQuque(final String topic, final int queueId);


    /**
     * 获取指定队列最小Offset 如果队列不存在，返回-1
     */
    public long getMinOffsetInQuque(final String topic, final int queueId);


    /**
     * 根据消息时间获取某个队列中对应的offset 1、如果指定时间（包含之前之后）有对应的消息，则获取距离此时间最近的offset（优先选择之前）
     * 2、如果指定时间无对应消息，则返回0
     */
    public long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);


    /**
     * 通过物理队列Offset，查询消息。 如果发生错误，则返回null
     */
    public MetaMessageWrapper lookMessageByOffset(final long phyOffset);


    /**
     * 获取运行时统计数据
     */
    public String getRunningDataInfo();


    /**
     * 获取物理队列最大offset
     */
    public long getMaxPhyOffset();


    /**
     * 获取队列中最早的消息时间
     */
    public long getEarliestMessageTime(final String topic, final int queueId);


    /**
     * 获取队列中的消息总数
     */
    public long getMessageTotalInQueue(final String topic, final int queueId);


    /**
     * 数据复制使用：获取物理队列数据
     */
    public SelectMapedBufferResult getPhyQueueData(final long offset);


    /**
     * 数据复制使用：向物理队列追加数据，并分发至各个逻辑队列
     */
    public boolean appendToPhyQueue(final long startOffset, final byte[] data);


    /**
     * 手动触发删除文件
     */
    public void excuteDeleteFilesManualy();
}
