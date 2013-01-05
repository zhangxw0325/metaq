/**
 * $Id: RunningFlags.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

/**
 * 存储层运行状态位
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class RunningFlags {
    // 禁止读权限
    private static final int NotReadableBit = 0x1;
    // 禁止写权限
    private static final int NotWriteableBit = 0x2;
    // 逻辑队列是否发生错误
    private static final int WriteLogicsQueueErrorBit = 0x4;
    // 磁盘空间不足
    private static final int DiskFullBit = 0x8;

    private volatile int flagBits = 0;


    public int getFlagBits() {
        return flagBits;
    }


    public RunningFlags() {
    }


    public boolean isReadable() {
        if ((this.flagBits & NotReadableBit) == 0) {
            return true;
        }

        return false;
    }


    public boolean isWriteable() {
        if ((this.flagBits & (NotWriteableBit | WriteLogicsQueueErrorBit | DiskFullBit)) == 0) {
            return true;
        }

        return false;
    }


    public boolean getAndMakeReadable() {
        boolean result = this.isReadable();
        if (!result) {
            this.flagBits &= ~NotReadableBit;
        }
        return result;
    }


    public boolean getAndMakeNotReadable() {
        boolean result = this.isReadable();
        if (result) {
            this.flagBits |= NotReadableBit;
        }
        return result;
    }


    public boolean getAndMakeWriteable() {
        boolean result = this.isWriteable();
        if (!result) {
            this.flagBits &= ~NotWriteableBit;
        }
        return result;
    }


    public boolean getAndMakeNotWriteable() {
        boolean result = this.isWriteable();
        if (result) {
            this.flagBits |= NotWriteableBit;
        }
        return result;
    }


    public void makeLogicsQueueError() {
        this.flagBits |= WriteLogicsQueueErrorBit;
    }


    public boolean isLogicsQueueError() {
        if ((this.flagBits & WriteLogicsQueueErrorBit) == WriteLogicsQueueErrorBit) {
            return true;
        }

        return false;
    }


    /**
     * 返回Disk是否正常
     */
    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits |= DiskFullBit;
        return result;
    }


    /**
     * 返回Disk是否正常
     */
    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits &= ~DiskFullBit;
        return result;
    }
}
