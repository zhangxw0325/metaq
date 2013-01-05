package com.taobao.metamorphosis.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class ZkUtilsUnitTest {
    private ZkClient client;
    private DiamondManager diamondManager;
    private ZKConfig zkConfig;


    @Before
    public void setUp() {
        this.diamondManager = new DefaultDiamondManager(null, "metamorphosis.testZkConfig", (ManagerListener) null);
        this.zkConfig = DiamondUtils.getZkConfig(this.diamondManager, 10000);
        this.client =
                new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
    }


    @Test
    public void testCreateEphemeralPathReadDataDelete() throws Exception {
        final String path = "/meta/test";
        final String data = "data";
        assertFalse(this.client.exists(path));
        ZkUtils.createEphemeralPath(this.client, path, data);
        assertTrue(this.client.exists(path));
        assertEquals(data, ZkUtils.readData(this.client, path));
        ZkUtils.deletePath(this.client, path);
        assertFalse(this.client.exists(path));
    }


    @Test(expected = ZkNodeExistsException.class)
    public void testCreateEphemeralDuplicate() throws Exception {
        final String path = "/meta/test";
        final String data = "data";
        assertFalse(this.client.exists(path));
        ZkUtils.createEphemeralPath(this.client, path, data);
        assertTrue(this.client.exists(path));
        ZkUtils.createEphemeralPath(this.client, path, data);
    }


    @Test
    public void testCreateEphemeralPathExpectConflictReadDelete() throws Exception {
        final String path = "/meta/test";
        final String data = "data";
        assertFalse(this.client.exists(path));
        ZkUtils.createEphemeralPath(this.client, path, data);
        assertTrue(this.client.exists(path));

        // create twice,no problem
        ZkUtils.createEphemeralPathExpectConflict(this.client, path, data);
        assertTrue(this.client.exists(path));
        assertEquals(data, ZkUtils.readData(this.client, path));
        ZkUtils.deletePath(this.client, path);
        assertFalse(this.client.exists(path));
    }


    @Test(expected = ZkNodeExistsException.class)
    public void testCreateEphemeralPathExpectConflictDifferentData() throws Exception {
        final String path = "/meta/test";
        final String data = "data";
        assertFalse(this.client.exists(path));
        ZkUtils.createEphemeralPath(this.client, path, data);
        assertTrue(this.client.exists(path));

        // create twice with different data
        ZkUtils.createEphemeralPathExpectConflict(this.client, path, "new data");
    }


    @Test
    public void testMakeSurePersistePathExistsDelete() throws Exception {
        final String path = "/meta/testMakeSurePersistePathExists";
        assertFalse(this.client.exists(path));
        ZkUtils.makeSurePersistentPathExists(this.client, path);
        assertTrue(this.client.exists(path));
        // Call twice,no problem
        ZkUtils.makeSurePersistentPathExists(this.client, path);
        assertTrue(this.client.exists(path));
        ZkUtils.deletePath(this.client, path);
    }


    @Test(expected = ZkNoNodeException.class)
    public void testReadDataNotExists() {
        final String path = "/meta/testReadDataNotExists";
        ZkUtils.readData(this.client, path);
    }


    @Test
    public void testGetLastPart() {
        assertEquals("world", ZkUtils.getLastPart("/meta/hello/world"));
        assertEquals("hello", ZkUtils.getLastPart("/meta/hello"));
        assertEquals("meta", ZkUtils.getLastPart("/meta"));
        assertNull(ZkUtils.getLastPart("meta"));
        assertNull(ZkUtils.getLastPart(null));
        assertNull(ZkUtils.getLastPart(""));
    }


    @Test
    public void testUpdatePersistentPathReadDelete() throws Exception {

        final String path = "/meta/testUpdatePersistentPathReadDelete";
        final String data = "data";
        assertFalse(ZkUtils.pathExists(this.client, path));
        ZkUtils.updatePersistentPath(this.client, path, data);
        assertTrue(ZkUtils.pathExists(this.client, path));
        assertEquals(data, ZkUtils.readData(this.client, path));
        ZkUtils.updatePersistentPath(this.client, path, "new data");
        assertEquals("new data", ZkUtils.readData(this.client, path));
        ZkUtils.deletePath(this.client, path);
        assertFalse(ZkUtils.pathExists(this.client, path));
    }


    @Test
    public void testUpdateEphemeralPathReadCloseRead() throws Exception {
        final String path = "/meta/testUpdateEphemeralPathReadCloseRead";
        final String data = "data";
        assertFalse(ZkUtils.pathExists(this.client, path));
        ZkUtils.updateEphemeralPath(this.client, path, data);
        assertTrue(ZkUtils.pathExists(this.client, path));
        assertEquals(data, ZkUtils.readData(this.client, path));

        this.client.close();
        Thread.sleep(this.zkConfig.zkSessionTimeoutMs);
        this.client =
                new ZkClient(this.zkConfig.zkConnect, this.zkConfig.zkSessionTimeoutMs,
                    this.zkConfig.zkConnectionTimeoutMs, new ZkUtils.StringSerializer());
        assertFalse(ZkUtils.pathExists(this.client, path));
        ZkUtils.updateEphemeralPath(this.client, path, data);
        assertTrue(ZkUtils.pathExists(this.client, path));
        assertEquals(data, ZkUtils.readData(this.client, path));

    }


    @Test
    public void testDeletePathRecursive() throws Exception {
        final String path = "/meta/1/2/3/4/testUpdateEphemeralPathReadCloseRead";
        final String data = "data";
        // assertFalse(ZkUtils.pathExists(this.client, path));
        ZkUtils.updatePersistentPath(this.client, path, data);
        assertTrue(ZkUtils.pathExists(this.client, path));

        try {
            ZkUtils.deletePath(this.client, "/meta");
            fail();
        }
        catch (final ZkException e) {

        }
        assertTrue(ZkUtils.pathExists(this.client, "/meta/1/2/3/4"));
        ZkUtils.updatePersistentPath(this.client, path, data);
        ZkUtils.deletePathRecursive(this.client, "/meta");
        assertFalse(ZkUtils.pathExists(this.client, path));
        assertFalse(ZkUtils.pathExists(this.client, "/meta/1/2/3/4"));
        assertFalse(ZkUtils.pathExists(this.client, "/meta/1/2/3"));
        assertFalse(ZkUtils.pathExists(this.client, "/meta/1/2"));
        assertFalse(ZkUtils.pathExists(this.client, "/meta/1"));
        assertFalse(ZkUtils.pathExists(this.client, "/meta"));
    }


    @Test
    public void testReadDataMayBeNullNotExists() {
        final String path = "/meta/testReadDataMayBeNullNotExists";
        assertNull(ZkUtils.readDataMaybeNull(this.client, path));
    }


    @After
    public void tearDown() throws Exception {
        this.diamondManager.close();
        this.client.close();
    }

}
