package com.taobao.metamorphosis.tail4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.diamond.io.FileSystem;
import com.taobao.diamond.io.Path;
import com.taobao.diamond.io.watch.StandardWatchEventKind;
import com.taobao.diamond.io.watch.WatchEvent;
import com.taobao.diamond.io.watch.WatchKey;
import com.taobao.diamond.io.watch.WatchService;
import com.taobao.metamorphosis.tail4j.Config.LogConfig;


public class Scanner {

    private final WatchService watchService;

    private final LogConfig logConfig;

    private final ExecutorService singleExecutor = Executors.newSingleThreadExecutor();

    private volatile boolean isRun = true;

    private final BlockingDeque<String> fifoQueue = new LinkedBlockingDeque<String>();

    private FileChannel fc;

    private File currentLogFile;

    static final Log log = LogFactory.getLog(Scanner.class);

    private final Pattern logNamePat;


    public void close() throws IOException {
        this.isRun = false;
        this.singleExecutor.shutdown();
        if (this.fc != null && this.fc.isOpen()) {
            this.fc.close();
        }
        this.watchService.close();
    }


    public Scanner(LogConfig logConfig) {
        super();
        this.logConfig = logConfig;
        this.logNamePat = Pattern.compile(logConfig.logNamePattern);
        this.watchService = FileSystem.getDefault().newWatchService();
        this.watchService
            .register(new Path(new File(logConfig.logBasePath)), true, StandardWatchEventKind.ENTRY_CREATE);
        // 第一次运行，主动check
        this.checkAtFirst(this.watchService);
        this.singleExecutor.execute(new Runnable() {
            public void run() {
                log.info(">>>>>>已经开始监控目录" + Scanner.this.logConfig.logBasePath + "<<<<<<");
                // 无限循环等待事件
                while (Scanner.this.isRun) {
                    // 凭证
                    WatchKey key;
                    try {
                        key = Scanner.this.watchService.take();
                    }
                    catch (InterruptedException x) {
                        continue;
                    }
                    // reset，如果无效，跳出循环,无效可能是监听的目录被删除
                    if (!Scanner.this.processEvents(key)) {
                        log.error("reset unvalid,监控服务失效");
                        break;
                    }
                }
                log.info(">>>>>>退出监控目录" + Scanner.this.logConfig.logBasePath + "<<<<<<");
                Scanner.this.watchService.close();
            }

        });
    }


    private void checkAtFirst(final WatchService watcher) {
        watcher.check();
        WatchKey key = null;
        while ((key = watcher.poll()) != null) {
            this.processEvents(key);
        }
    }


    public File getCurrLogFile() {
        return this.currentLogFile;
    }


    public synchronized void closeChannel() throws IOException {
        if (this.fc == null) {
            throw new NoOpenFileChannelException();
        }
        String path = this.fifoQueue.poll();
        log.info("Closing file " + this.currentLogFile.getAbsolutePath());
        this.fc.close();
        this.fc = null;
        this.currentLogFile = null;
    }


    public int getQueueSize() {
        return this.fifoQueue.size();
    }


    public synchronized FileChannel getCurrChannel(long lastModify) throws IOException {
        if (this.fc == null) {
            // 跳过比上一次保存的checkpoint还旧的文件
            if (lastModify > 0) {
                String path = null;
                while ((path = this.fifoQueue.peek()) != null) {
                    File file = new File(path);
                    if (file.exists()) {
                        // 旧的文件，略过
                        if (file.lastModified() < lastModify) {
                            this.fifoQueue.poll();
                        }
                        else {
                            break;
                        }
                    }
                    // 临时文件暂时不存在，可以接受
                    else if (path.equals(this.logConfig.tmpLogFullPath)) {
                        break;
                    }
                    else {
                        // 不存在的文件，忽悠
                        this.fifoQueue.poll();
                    }
                }
            }
            String path = this.fifoQueue.peek();
            if (path == null) {
                return null;
            }
            this.currentLogFile = new File(path);
            if (!this.currentLogFile.exists()) {
                return null;
            }
            this.fc = new RandomAccessFile(this.currentLogFile, "r").getChannel();
            log.info("Opening file " + path);
        }
        return this.fc;
    }


    /**
     * 处理触发的事件
     * 
     * @param key
     * @return
     */
    @SuppressWarnings( { "unchecked" })
    private synchronized boolean processEvents(WatchKey key) {
        List<Path> newList = new ArrayList<Path>();
        /**
         * 获取事件集合
         */
        for (WatchEvent<?> event : key.pollEvents()) {
            WatchEvent<Path> ev = (WatchEvent<Path>) event;
            Path eventPath = ev.context();
            String realPath = eventPath.getAbsolutePath();
            if (ev.kind() == StandardWatchEventKind.ENTRY_CREATE || ev.kind() == StandardWatchEventKind.ENTRY_MODIFY) {
                if (this.logNamePat.matcher(realPath).matches()) {
                    newList.add(eventPath);
                }
            }

        }
        // 从旧到新排列
        Collections.sort(newList, new Comparator<Path>() {
            public int compare(Path o1, Path o2) {
                long result = o1.lastModified() - o2.lastModified();
                if (result == 0) {
                    return 0;
                }
                else {
                    return result > 0 ? 1 : -1;
                }
            }

        });
        File tail = null;
        if (!this.fifoQueue.isEmpty()) {
            tail = new File(this.fifoQueue.peekLast());
        }
        // 加入队列,从旧到新
        for (Path path : newList) {
            String absolutePath = path.getAbsolutePath();

            // watch service可能感知到添加tmp文件的事件，如果队列中已经有临时文件，则忽悠
            if (absolutePath.equals(this.logConfig.tmpLogFullPath)
                    && this.fifoQueue.contains(this.logConfig.tmpLogFullPath)) {
                continue;
            }

            // tmp mode,当前正在读临时文件，新加入的文件是rename后的文件，但是总作为临时文件加入
            if (this.currentLogFile != null
                    && this.currentLogFile.getAbsolutePath().equals(this.logConfig.tmpLogFullPath)) {
                log.info("加入文件" + this.logConfig.tmpLogFullPath);
                this.fifoQueue.offer(this.logConfig.tmpLogFullPath);
            }
            else {
                // 只有比尾部的文件还旧的文件才加入
                if (tail == null || path.lastModified() > tail.lastModified()) {
                    log.info("加入文件" + absolutePath);
                    this.fifoQueue.offer(absolutePath);
                }
                else {
                    log.info("忽略文件" + absolutePath);
                }
            }
        }
        return key.reset();
    }

}
