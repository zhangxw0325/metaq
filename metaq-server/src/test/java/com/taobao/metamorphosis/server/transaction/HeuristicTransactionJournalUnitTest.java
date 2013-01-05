package com.taobao.metamorphosis.server.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class HeuristicTransactionJournalUnitTest {
    public String getTempPath() {
        final String path = System.getProperty("java.io.tmpdir");
        final String pathname = path + File.separator + "meta";
        return pathname;
    }

    protected String path;
    private HeuristicTransactionJournal journal;


    @Before
    public void setUp() throws Exception {
        this.path = this.getTempPath();
        FileUtils.deleteDirectory(new File(this.path));
        this.journal = new HeuristicTransactionJournal(this.path);
        System.out.println(this.path);
    }


    @After
    public void tearDown() throws Exception {
        this.journal.close();
    }


    @Test
    public void testReadWriteRead() throws Exception {
        assertNull(this.journal.read());
        this.journal.write(1);
        assertEquals(1, this.journal.read());
        assertEquals(1, this.journal.read());

        final Date date = new Date();
        this.journal.write(date);
        assertEquals(date, this.journal.read());

        this.journal.write(null);
        assertEquals(date, this.journal.read());
    }
}
