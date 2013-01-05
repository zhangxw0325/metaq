/**
 * $Id: MetaStoreConfig.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.metamorphosis.utils.MetaMBeanServer;

/**
 * 存储配置类
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class MetaStoreConfig implements MetaStoreConfigMBean {
    static final Log log = LogFactory.getLog(MetaStoreConfig.class);
    // 是否是Master角色，如果是Slave，请设置成false
    private boolean master = true;
    // 物理队列存储目录
    private String storePathPhysic = System.getProperty("user.home") + File.separator + "metastore"
            + File.separator + "physic";
    // 逻辑队列存储目录
    private String storePathLogics = System.getProperty("user.home") + File.separator + "metastore"
            + File.separator + "logics";
    // 异常退出产生的文件
    private String storeCheckpoint = System.getProperty("user.home") + File.separator + "metastore"
            + File.separator + "metaStoreCheckpoint";
    // 异常退出产生的文件
    private String abortFile = System.getProperty("user.home") + File.separator + "metastore" + File.separator
            + "metaStoreAbort";
    // 物理队列每个文件大小 1G
    private int mapedFileSizePhysic = 1024 * 1024 * 1024;
    // 逻辑队列每个文件大小 2M
    private int mapedFileSizeLogics = 1024 * 1024 * 2;
    // 物理队列刷盘间隔时间（单位毫秒）
    private int flushIntervalPhysic = 1000;
    // 逻辑队列刷盘间隔时间（单位毫秒）
    private int flushIntervalLogics = 1000;
    // 清理资源间隔时间（单位毫秒）
    private int cleanResourceInterval = 10000;
    // 删除多个物理文件的间隔时间（单位毫秒）
    private int deletePhysicFilesInterval = 100;
    // 删除多个逻辑文件的间隔时间（单位毫秒）
    private int deleteLogicsFilesInterval = 100;
    // 强制删除文件间隔时间（单位毫秒）
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    // 定期检查Hanged文件间隔时间（单位毫秒）
    private int redeleteHangedFileInterval = 1000 * 120;
    // 何时触发删除文件, 默认凌晨4点删除文件
    private String deleteWhen = "04";
    // 磁盘空间最大使用率
    private int diskMaxUsedSpaceRatio = 75;
    // 文件保留时间（单位小时）
    private int fileReservedTime = 12;
    // 是否开启GrouCommit功能
    private boolean groupCommitEnable = false;
    // GrouCommit 等待超时时间（单位毫秒）
    private int groupCommitTimeout = 1000 * 5;
    // 写消息索引到逻辑队列，缓冲区高水位，超过则开始流控
    private int putMsgIndexHightWater = 400000;
    // 最大消息大小，默认512K
    private int maxMessageSize = 1024 * 512;
    // 重启时，是否校验CRC
    private boolean checkCRCOnRecover = true;
    // 刷物理队列，至少刷几个PAGE
    private int flushPhysicQueueLeastPages = 4;
    // 刷逻辑队列，至少刷几个PAGE
    private int flushLogicsQueueLeastPages = 2;
    // 刷物理队列，彻底刷盘间隔时间
    private int flushPhysicQueueThoroughInterval = 1000 * 10;
    // 刷逻辑队列，彻底刷盘间隔时间
    private int flushLogicsQueueThoroughInterval = 1000 * 60;
    // 最大被拉取的消息字节数，消息在内存
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    // 最大被拉取的消息个数，消息在内存
    private int maxTransferCountOnMessageInMemory = 32;
    // 最大被拉取的消息字节数，消息在磁盘
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    // 最大被拉取的消息个数，消息在磁盘
    private int maxTransferCountOnMessageInDisk = 8;
    // 当前进程可用物理内存大小，单位G
    private int totalPhysicMemory = 5;


    public int getMapedFileSizePhysic() {
        return mapedFileSizePhysic;
    }


    public void setMapedFileSizePhysic(int mapedFileSizePhysic) {
        this.mapedFileSizePhysic = mapedFileSizePhysic;
    }


    public int getMapedFileSizeLogics() {
        // 此处需要向上取整
        int factor = (int) Math.ceil(this.mapedFileSizeLogics / (MetaQueueLogistic.StoreUnitSize * 1.0));
        return (int) (factor * MetaQueueLogistic.StoreUnitSize);
    }


    public void setMapedFileSizeLogics(int mapedFileSizeLogics) {
        this.mapedFileSizeLogics = mapedFileSizeLogics;
    }


    public int getFlushIntervalPhysic() {
        return flushIntervalPhysic;
    }


    public void setFlushIntervalPhysic(int flushIntervalPhysic) {
        this.flushIntervalPhysic = flushIntervalPhysic;
    }


    public int getFlushIntervalLogics() {
        return flushIntervalLogics;
    }


    public void setFlushIntervalLogics(int flushIntervalLogics) {
        this.flushIntervalLogics = flushIntervalLogics;
    }


    public boolean isGroupCommitEnable() {
        return groupCommitEnable;
    }


    public void setGroupCommitEnable(boolean groupCommitEnable) {
        this.groupCommitEnable = groupCommitEnable;
    }


    public int getGroupCommitTimeout() {
        return groupCommitTimeout;
    }


    public boolean getGroupCommitEnable() {
        return groupCommitEnable;
    }


    public void setGroupCommitTimeout(int groupCommitTimeout) {
        this.groupCommitTimeout = groupCommitTimeout;
    }


    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }


    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }


    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }


    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }


    public int getMaxMessageSize() {
        return maxMessageSize;
    }


    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }


    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }


    public boolean getCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }


    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }


    public String getStorePathPhysic() {
        return storePathPhysic;
    }


    public void setStorePathPhysic(String storePathPhysic) {
        this.storePathPhysic = storePathPhysic;
    }


    public String getStorePathLogics() {
        return storePathLogics;
    }


    public void setStorePathLogics(String storePathLogics) {
        this.storePathLogics = storePathLogics;
    }


    public String getAbortFile() {
        return abortFile;
    }


    public void setAbortFile(String abortFile) {
        this.abortFile = abortFile;
    }


    public String getDeleteWhen() {
        return deleteWhen;
    }


    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }


    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10)
            return 10;

        if (this.diskMaxUsedSpaceRatio > 95)
            return 95;

        return diskMaxUsedSpaceRatio;
    }


    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }


    public int getDeletePhysicFilesInterval() {
        return deletePhysicFilesInterval;
    }


    public void setDeletePhysicFilesInterval(int deletePhysicFilesInterval) {
        this.deletePhysicFilesInterval = deletePhysicFilesInterval;
    }


    public int getDeleteLogicsFilesInterval() {
        return deleteLogicsFilesInterval;
    }


    public void setDeleteLogicsFilesInterval(int deleteLogicsFilesInterval) {
        this.deleteLogicsFilesInterval = deleteLogicsFilesInterval;
    }


    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }


    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }


    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }


    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }


    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }


    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }


    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }


    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }


    public int getTotalPhysicMemory() {
        return totalPhysicMemory;
    }


    public void setTotalPhysicMemory(int totalPhysicMemory) {
        this.totalPhysicMemory = totalPhysicMemory;
    }


    public int getFlushPhysicQueueLeastPages() {
        return flushPhysicQueueLeastPages;
    }


    public void setFlushPhysicQueueLeastPages(int flushPhysicQueueLeastPages) {
        this.flushPhysicQueueLeastPages = flushPhysicQueueLeastPages;
    }


    public int getFlushLogicsQueueLeastPages() {
        return flushLogicsQueueLeastPages;
    }


    public void setFlushLogicsQueueLeastPages(int flushLogicsQueueLeastPages) {
        this.flushLogicsQueueLeastPages = flushLogicsQueueLeastPages;
    }


    public int getFlushPhysicQueueThoroughInterval() {
        return flushPhysicQueueThoroughInterval;
    }


    public void setFlushPhysicQueueThoroughInterval(int flushPhysicQueueThoroughInterval) {
        this.flushPhysicQueueThoroughInterval = flushPhysicQueueThoroughInterval;
    }


    public int getFlushLogicsQueueThoroughInterval() {
        return flushLogicsQueueThoroughInterval;
    }


    public void setFlushLogicsQueueThoroughInterval(int flushLogicsQueueThoroughInterval) {
        this.flushLogicsQueueThoroughInterval = flushLogicsQueueThoroughInterval;
    }


    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }


    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }


    public String getStoreCheckpoint() {
        return storeCheckpoint;
    }


    public void setStoreCheckpoint(String storeCheckpoint) {
        this.storeCheckpoint = storeCheckpoint;
    }


    public boolean isMaster() {
        return master;
    }


    public void setMaster(boolean master) {
        this.master = master;
    }


    public boolean getMaster() {
        return this.master;
    }


    public int getFileReservedTime() {
        return fileReservedTime;
    }


    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }


    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }


    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }


    @Override
    public void reload(String configPath) {
        MetaStoreConfig msc = MetaStoreConfig.createMetaStoreConfig(configPath, false);
        Method[] methods = MetaStoreConfig.class.getMethods();
        for (Method setMethod : methods) {
            String setName = setMethod.getName();
            if (setName.startsWith("set")) {
                String getName = setName.replaceFirst("set", "get");
                try {
                    Method getMethod = MetaStoreConfig.class.getMethod(getName, new Class[] {});
                    setMethod.invoke(this, new Object[] { getMethod.invoke(msc, new Object[] {}) });
                }
                catch (NoSuchMethodException e) {

                }
                catch (Exception e) {
                    log.error("invoke method error. method=set/get" + setName.substring(3), e);
                }
            }
        }
        log.info("reload metastoreconfig " + configPath + "success at " + new Date());
    }


    public static MetaStoreConfig createMetaStoreConfig(String configPath, boolean registerMBean) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext(configPath);
        MetaStoreConfig msc = (MetaStoreConfig) ctx.getBean("metaStoreConfig");
        if (registerMBean) {
            MetaMBeanServer.registMBean(msc, null);
        }
        return msc;
    }

}
