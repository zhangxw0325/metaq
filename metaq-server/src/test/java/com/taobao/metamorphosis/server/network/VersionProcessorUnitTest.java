package com.taobao.metamorphosis.server.network;

import org.easymock.classextension.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.VersionCommand;
import com.taobao.metamorphosis.server.utils.BuildProperties;


public class VersionProcessorUnitTest extends BaseProcessorUnitTest {

    private VersionProcessor processor;


    @Before
    public void setUp() {
        this.mock();
        this.processor = new VersionProcessor(this.commandProcessor);
    }


    @Test
    public void testHandleRequest() throws Exception {
        final int opaque = 1;
        this.conn.response(new BooleanCommand(opaque, HttpStatus.Success, BuildProperties.VERSION));
        EasyMock.expectLastCall();
        this.mocksControl.replay();
        this.processor.handleRequest(new VersionCommand(opaque), this.conn);
        this.mocksControl.verify();
    }
}
