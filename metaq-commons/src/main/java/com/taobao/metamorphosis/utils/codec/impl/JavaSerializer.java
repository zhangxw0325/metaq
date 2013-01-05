package com.taobao.metamorphosis.utils.codec.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.utils.codec.Serializer;


/**
 * 
 * @author wuxin
 * @since 1.0, 2009-10-20 ÉÏÎç09:46:12
 */
public class JavaSerializer implements Serializer {

    private final Logger logger = Logger.getLogger(JavaSerializer.class);


    /**
     * @see com.taobao.notify.codec.Serializer#encodeObject(Object)
     */
    @Override
    public byte[] encodeObject(final Object objContent) throws IOException {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream output = null;
        try {
            baos = new ByteArrayOutputStream(1024);
            output = new ObjectOutputStream(baos);
            output.writeObject(objContent);
        }
        catch (final IOException ex) {
            throw ex;

        }
        finally {
            if (output != null) {
                try {
                    output.close();
                    if (baos != null) {
                        baos.close();
                    }
                }
                catch (final IOException ex) {
                    this.logger.error("Failed to close stream.", ex);
                }
            }
        }
        return baos != null ? baos.toByteArray() : null;
    }
}
