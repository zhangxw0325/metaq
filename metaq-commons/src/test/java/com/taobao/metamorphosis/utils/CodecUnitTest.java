package com.taobao.metamorphosis.utils;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.utils.codec.CodecBuilder;
import com.taobao.metamorphosis.utils.codec.CodecBuilder.Codec_Type;
import com.taobao.metamorphosis.utils.codec.Deserializer;
import com.taobao.metamorphosis.utils.codec.Serializer;


/**
 * 
 * @author boyan
 * @since 1.0, 2009-10-20 ÉÏÎç10:28:18
 */
public class CodecUnitTest {
    private Message msg = null;


    @Before
    public void setUp() {
        this.msg = new Message("CodecUnitTest", "hello world".getBytes());
        this.msg.setAttribute("test attribute");
    }


    @Test
    public void testJavaEncoder() throws Exception {
        final Serializer encoder = CodecBuilder.buildSerializer(Codec_Type.JAVA);
        byte buf[] = null;

        for (int i = 0; i < 100000; i++) {
            buf = encoder.encodeObject(this.msg);
        }

        Assert.assertTrue(buf != null && buf.length > 0);
    }


    @Test
    public void testJavaDecoder() throws Exception {
        final Serializer encoder = CodecBuilder.buildSerializer(Codec_Type.JAVA);
        final Deserializer decoder = CodecBuilder.buildDeserializer(Codec_Type.JAVA);
        final byte buf[] = encoder.encodeObject(this.msg);
        final Message entity1 = (Message) decoder.decodeObject(buf);

        Assert.assertEquals(this.msg, entity1);
    }


    @Test
    public void testHessianEncoder() throws IOException {
        final Serializer encoder = CodecBuilder.buildSerializer(Codec_Type.HESSIAN1);
        byte buf[] = null;

        for (int i = 0; i < 100000; i++) {
            buf = encoder.encodeObject(this.msg);
        }

        Assert.assertTrue(buf != null && buf.length > 0);
    }


    @Test
    public void testHessianDecoder() throws Exception {
        final Deserializer decoder = CodecBuilder.buildDeserializer(Codec_Type.HESSIAN1);
        final Serializer encoder = CodecBuilder.buildSerializer(Codec_Type.HESSIAN1);

        final byte buf[] = encoder.encodeObject(this.msg);
        final Message entity1 = (Message) decoder.decodeObject(buf);

        Assert.assertEquals(this.msg, entity1);
    }
}
