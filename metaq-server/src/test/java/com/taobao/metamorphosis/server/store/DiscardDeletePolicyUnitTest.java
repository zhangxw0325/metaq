package com.taobao.metamorphosis.server.store;

import static org.junit.Assert.*;

import java.io.File;

import org.easymock.classextension.EasyMock;
import org.junit.Test;


public class DiscardDeletePolicyUnitTest {
    private DiscardDeletePolicy policy;


    @Test
    public void testCanDelete() {
        this.policy = new DiscardDeletePolicy();
        this.policy.setMaxReservedTime(1000L);
        File file = EasyMock.createMock(File.class);

        EasyMock.expect(file.lastModified()).andReturn(System.currentTimeMillis() - 2000);
        EasyMock.replay(file);
        assertTrue(this.policy.canDelete(file, System.currentTimeMillis()));
        EasyMock.verify(file);
    }


    @Test
    public void testInit() {
        this.policy = new DiscardDeletePolicy();
        this.policy.init("12", "24");
        assertEquals(12 * 3600 * 1000, this.policy.getMaxReservedTime());
    }


    @Test
    public void testProcess() throws Exception {
        this.policy = new DiscardDeletePolicy();
        File file = File.createTempFile("DailyDeletePolicyUnitTest", ".test");
        assertTrue(file.exists());
        this.policy.process(file);
        assertFalse(file.exists());
    }
}
