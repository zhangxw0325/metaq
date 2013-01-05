/**
 * $Id: StoreCheckpointTest.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.store;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class StoreCheckpointTest {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }


    @Test
    public void test_write_read() {
        try {
            StoreCheckpoint storeCheckpoint = new StoreCheckpoint("./a/b/0000");
            long physicMsgTimestamp = 0xAABB;
            long logicsMsgTimestamp = 0xCCDD;
            storeCheckpoint.setPhysicMsgTimestamp(physicMsgTimestamp);
            storeCheckpoint.flush(logicsMsgTimestamp);

            assertTrue(physicMsgTimestamp == storeCheckpoint.getMinTimestamp());

            storeCheckpoint.shutdown();

            storeCheckpoint = new StoreCheckpoint("a/b/0000");
            assertTrue(physicMsgTimestamp == storeCheckpoint.getPhysicMsgTimestamp());
            assertTrue(logicsMsgTimestamp == storeCheckpoint.getLogicsMsgTimestamp());
        }
        catch (Throwable e) {
            e.printStackTrace();
            assertTrue(false);
        }

    }
}
