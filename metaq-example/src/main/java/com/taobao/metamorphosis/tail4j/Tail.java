package com.taobao.metamorphosis.tail4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.util.ByteBufferMatcher;
import com.taobao.gecko.core.util.ShiftAndByteBufferMatcher;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.tail4j.Config.LogConfig;


/**
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class Tail extends Thread {
    private final Scanner scanner;
    private final CheckPoint checkPoint;
    private final Sender sender;
    private volatile boolean shutdown = false;
    static final Log log = LogFactory.getLog(Tail.class);
    private final Config config;
    private final LogConfig logConfig;

    static final ByteBufferMatcher[] matchers = { new ShiftAndByteBufferMatcher(IoBuffer.wrap("\r\n".getBytes())),
                                                 new ShiftAndByteBufferMatcher(IoBuffer.wrap("\n".getBytes())),
                                                 new ShiftAndByteBufferMatcher(IoBuffer.wrap("\r".getBytes())) };

    static final int[] skips = { 2, 1, 1 };


    public Tail(Config config, LogConfig logConfig, MessageSessionFactory sessionFactory) throws IOException {
        this.scanner = new Scanner(logConfig);
        this.sender = new Sender(logConfig, sessionFactory.createProducer(logConfig.ordered));
        this.checkPoint = new CheckPoint(config, logConfig);
        this.config = config;
        this.logConfig = logConfig;
    }


    public void shutdown() {
        this.shutdown = true;
    }


    @Override
    public void run() {
        try {
            while (!this.shutdown) {
                try {
                    FileChannel fc = this.scanner.getCurrChannel(this.checkPoint.getLastModify());
                    if (fc == null) {
                        Thread.sleep(this.logConfig.timeout);
                        continue;
                    }
                    long position = this.getPosition(fc);
                    // 定位到上一次读的位置
                    fc.position(position);
                    IoBuffer buf = IoBuffer.allocate(this.config.getMaxBufSize());
                    int read = fc.read(buf.buf());
                    if (read == -1) {
                        if (this.scanner.getQueueSize() > 1) {
                            this.checkPoint.setPosition(0);
                            this.checkPoint.save();
                            this.scanner.closeChannel();
                            if (this.testFileChannel != null) {
                                this.testFileChannel.close();
                                this.testFileChannel = null;
                            }
                            continue;
                        }
                        else {
                            Thread.sleep(this.logConfig.timeout);
                            continue;
                        }
                    }
                    buf.flip();
                    MatchResult result = null;
                    int sent = 0;
                    while ((result = this.match(buf)) != null) {
                        int index = result.index;
                        if (index >= 0) {
                            byte[] data = new byte[index - buf.position() + result.skip];
                            buf.get(data);
                            try {
                                this.send(data);
                                sent += data.length;
                            }
                            catch (InterruptedException e) {
                                throw e;
                            }
                            catch (Exception e) {
                                log.error("Send message failed", e);
                                break;
                            }
                        }
                        else {
                            break;
                        }
                    }
                    // 递增位置
                    position += sent;
                    // check point
                    if (sent > 0) {
                        this.checkPoint.setPosition(position);
                        this.checkPoint.save();
                    }
                }
                catch (InterruptedException e) {

                }
                catch (IOException t) {
                    log.error("读取文件失败", t);
                }
                catch (Throwable t) {
                    log.error("tail4j运行错误", t);
                }
            }
        }
        finally {
            this.close();
        }
    }


    private void close() {
        try {
            this.scanner.close();
            this.sender.close();
            this.checkPoint.close();
            if (this.testFileChannel != null) {
                this.testFileChannel.close();
            }
        }
        catch (IOException e) {
            log.error("Close failed", e);
        }
    }

    private FileChannel testFileChannel;


    private void send(byte[] data) throws IOException, InterruptedException {
        String currentLogPath = this.scanner.getCurrLogFile().getAbsolutePath();
        // 是否使用测试目录
        if (!StringUtils.isBlank(this.config.getTestPath())) {
            // 求相对路径
            String canonicalPath = currentLogPath.substring(this.logConfig.logBasePath.length());

            if (this.testFileChannel == null) {
                final File file = new File(this.config.getTestPath() + File.separator + canonicalPath);
                File parent = file.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                if (!file.exists()) {
                    file.createNewFile();
                }
                this.testFileChannel = new RandomAccessFile(file, "rw").getChannel();
                // move to tail
                this.testFileChannel.position(this.testFileChannel.size());
            }
            final ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining()) {
                this.testFileChannel.write(buf);
            }

        }
        else {
            this.sender.send(data, currentLogPath);
        }
    }


    private long getPosition(FileChannel fc) throws IOException {
        String savePos = this.checkPoint.getPosition();
        long result = 0L;
        if (!StringUtils.isBlank(savePos)) {
            result = Long.parseLong(savePos);
        }
        if (result == 0) {
            this.checkPoint.setLastModify(this.scanner.getCurrLogFile().lastModified());
        }
        return result;

    }

    private static class MatchResult {
        int index;
        int skip;


        public MatchResult(int index, int skip) {
            super();
            this.index = index;
            this.skip = skip;
        }

    }


    public MatchResult match(IoBuffer buf) {
        int i = 0;
        for (ByteBufferMatcher matcher : matchers) {
            int index = matcher.matchFirst(buf);
            i++;
            if (index >= 0) {
                return new MatchResult(index, skips[i - 1]);
            }
        }
        return new MatchResult(-1, 0);
    }
}
