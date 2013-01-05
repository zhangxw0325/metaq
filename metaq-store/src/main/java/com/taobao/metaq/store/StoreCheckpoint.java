/**
 * $Id: StoreCheckpoint.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

import com.taobao.metaq.commons.MetaUtil;

/**
 * 存储恢复时，从什么时间点开始恢复
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class StoreCheckpoint {
    private static final Logger log = Logger.getLogger(MetaStore.MetaStoreLogName);
    private volatile long physicMsgTimestamp = 0;
    private volatile long logicsMsgTimestamp = 0;

    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;


    public StoreCheckpoint(final String scpPath) throws IOException {
        File file = new File(scpPath);
        MapedFile.ensureDirOK(file.getParent());
        boolean fileExists = file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();
        this.mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, MapedFile.OS_PAGE_SIZE);

        if (fileExists) {
            log.info("store checkpoint file exists, " + scpPath);
            this.physicMsgTimestamp = this.mappedByteBuffer.getLong(0);
            this.logicsMsgTimestamp = this.mappedByteBuffer.getLong(8);

            log.info("store checkpoint file physicMsgTimestamp " + this.physicMsgTimestamp + ", "
                    + MetaUtil.timeMillisToHumanString(this.physicMsgTimestamp));
            log.info("store checkpoint file logicsMsgTimestamp " + this.logicsMsgTimestamp + ", "
                    + MetaUtil.timeMillisToHumanString(this.logicsMsgTimestamp));
        }
        else {
            log.info("store checkpoint file not exists, " + scpPath);
        }
    }


    public void flush(final long flushLogicsMsgTimestamp) {
        this.mappedByteBuffer.putLong(0, this.physicMsgTimestamp);
        this.mappedByteBuffer.putLong(8, flushLogicsMsgTimestamp);
        this.mappedByteBuffer.force();
    }


    public void flush() {
        this.flush(this.logicsMsgTimestamp);
    }


    public void shutdown() {
        this.flush();

        // unmap mappedByteBuffer
        MapedFile.clean(this.mappedByteBuffer);

        try {
            this.fileChannel.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    public long getPhysicMsgTimestamp() {
        return physicMsgTimestamp;
    }


    public void setPhysicMsgTimestamp(long physicMsgTimestamp) {
        this.physicMsgTimestamp = physicMsgTimestamp;
    }


    public long getLogicsMsgTimestamp() {
        return logicsMsgTimestamp;
    }


    public void setLogicsMsgTimestamp(long logicsMsgTimestamp) {
        this.logicsMsgTimestamp = logicsMsgTimestamp;
    }


    public long getMinTimestamp() {
        return Math.min(this.physicMsgTimestamp, this.logicsMsgTimestamp);
    }
}
