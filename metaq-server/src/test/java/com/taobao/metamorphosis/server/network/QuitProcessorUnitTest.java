package com.taobao.metamorphosis.server.network;

import org.easymock.classextension.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.network.QuitCommand;


public class QuitProcessorUnitTest extends BaseProcessorUnitTest {
    private QuitProcessor processor;


    @Before
    public void setUp() {
        this.mock();
        this.processor = new QuitProcessor(this.commandProcessor);
    }


    @Test
    public void testHandleRequest() throws Exception {
        this.conn.close(false);
        EasyMock.expectLastCall();
        this.mocksControl.replay();
        this.processor.handleRequest(new QuitCommand(), this.conn);
        this.mocksControl.verify();
    }
}
