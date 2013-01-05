package com.taobao.metamorphosis.server.store;

import static org.junit.Assert.*;

import org.junit.Test;

import com.taobao.metamorphosis.server.exception.UnknownDeletePolicyException;


public class DeletePolicyFactoryUnitTest {

    @Test(expected = UnknownDeletePolicyException.class)
    public void testGetDeletePolicyUnknow() {
        DeletePolicyFactory.getDeletePolicy("test");
    }


    @Test
    public void testGetDailyDeletePolicy() {
        DeletePolicy policy = DeletePolicyFactory.getDeletePolicy("delete,3");
        assertNotNull(policy);
        assertTrue(policy instanceof DiscardDeletePolicy);
        assertEquals(3 * 3600 * 1000, ((DiscardDeletePolicy) policy).getMaxReservedTime());
    }


    @Test
    public void testGetArchiveDeletePolicy() {
        DeletePolicy policy = DeletePolicyFactory.getDeletePolicy("archive,3,true");
        assertNotNull(policy);
        assertTrue(policy instanceof ArchiveDeletePolicy);
        assertEquals(3 * 3600 * 1000, ((ArchiveDeletePolicy) policy).getMaxReservedTime());
        assertTrue(((ArchiveDeletePolicy) policy).isCompress());
    }
}
