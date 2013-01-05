package com.taobao.metamorphosis.gregor.slave;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.command.RequestCommand;
import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RemotingContext;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.gecko.service.exception.NotifyRemotingException;


public class DummyConnection implements Connection {

    @Override
    public Future<Boolean> asyncSend(final RequestCommand requestCommand) throws NotifyRemotingException {

        return null;
    }


    // @Override
    // public Set<String> attributeKeySet() {
    // // TODO Auto-generated method stub
    // return null;
    // }

    @Override
    public Set<String> attributeKeySet() {
        return null;
    }


    @Override
    public void clearAttributes() {

    }


    @Override
    public void close(final boolean allowReconnect) throws NotifyRemotingException {

    }


    @Override
    public Object getAttribute(final String key) {

        return null;
    }


    @Override
    public Set<String> getGroupSet() {

        return null;
    }


    @Override
    public InetAddress getLocalAddress() {

        return null;
    }


    @Override
    public InetSocketAddress getRemoteSocketAddress() {

        return null;
    }


    @Override
    public RemotingContext getRemotingContext() {

        return null;
    }


    @Override
    public ResponseCommand invoke(final RequestCommand requestCommand, final long time, final TimeUnit timeUnit)
            throws InterruptedException, TimeoutException, NotifyRemotingException {

        return null;
    }


    @Override
    public ResponseCommand invoke(final RequestCommand request) throws InterruptedException, TimeoutException, NotifyRemotingException {

        return null;
    }


    @Override
    public boolean isConnected() {

        return false;
    }


    @Override
    public ByteOrder readBufferOrder() {

        return null;
    }


    @Override
    public void readBufferOrder(final ByteOrder byteOrder) {

    }


    @Override
    public void removeAttribute(final String key) {

    }


    @Override
    public void response(final Object responseCommand) throws NotifyRemotingException {

    }


    @Override
    public void send(final RequestCommand requestCommand, final SingleRequestCallBackListener listener, final long time,
            final TimeUnit timeUnit) throws NotifyRemotingException {

    }


    @Override
    public void send(final RequestCommand requestCommand, final SingleRequestCallBackListener listener) throws NotifyRemotingException {

    }


    @Override
    public void send(final RequestCommand requestCommand) throws NotifyRemotingException {

    }


    @Override
    public void setAttribute(final String key, final Object value) {

    }


    @Override
    public Object setAttributeIfAbsent(final String key, final Object value) {

        return null;
    }


    @Override
    public void setWriteInterruptibly(final boolean writeInterruptibly) {

    }


    @Override
    public void transferFrom(final IoBuffer head, final IoBuffer tail, final FileChannel channel, final long position, final long size) {

    }


    @Override
    public void transferFrom(IoBuffer head, IoBuffer tail, FileChannel channel, long position, long size, Integer opaque,
            SingleRequestCallBackListener listener, long time, TimeUnit unit) throws NotifyRemotingException {

    }


    @Override
    public void transferPageCache(IoBuffer head, IoBuffer tail, List<ByteBuffer> bufferList, long position, long size, Integer opaque,
            SingleRequestCallBackListener listener, long time, TimeUnit unit) throws NotifyRemotingException {
        // TODO Auto-generated method stub

    }

}
