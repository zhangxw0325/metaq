package com.taobao.metamorphosis.server.transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.store.JournalStore;
import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore;
import com.taobao.metamorphosis.server.utils.MetaConfig;


public abstract class BaseTransactionUnitTest {

    protected JournalTransactionStore transactionStore;
    protected JournalStore journalStore;
    protected MessageStoreManager messageStoreManager;
    protected String path;


    @Before
    public void setUp() throws Exception {
        this.path = this.getTempPath();
        FileUtils.deleteDirectory(new File(this.path));
        this.init(this.path);
    }


    public String getTempPath() {
        final String path = System.getProperty("java.io.tmpdir");
        final String pathname = path + File.separator + "meta";
        return pathname;
    }


    protected void init(final String path) throws Exception {

        final MetaConfig config = new MetaConfig();
        config.setFlushTxLogAtCommit(1);
        config.setNumPartitions(10);
        final List<String> topics = new ArrayList<String>();
        topics.add("topic1");
        topics.add("topic2");
        config.setTopics(topics);
        config.setDataPath(path);
        this.messageStoreManager = new MessageStoreManager(config, null);
        this.transactionStore = new JournalTransactionStore(path, this.messageStoreManager, config);
        this.journalStore = this.transactionStore.getJournalStore();
    }


    @After
    public void tearDown() {
        this.messageStoreManager.dispose();
        this.transactionStore.dispose();
    }

}
