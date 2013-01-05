package com.taobao.metamorphosis.utils.codec.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.caucho.hessian.io.HessianOutput;
import com.taobao.metamorphosis.utils.codec.Serializer;


/**
 * 
 * @author wuxin
 * @since 1.0, 2009-10-20 ÉÏÎç10:03:24
 */
public class Hessian1Serializer implements Serializer {

    private final Logger logger = Logger.getLogger(Hessian1Serializer.class);


    /**
     * @see com.taobao.notify.codec.Serializer#encodeObject(Object)
     */
    @Override
    public byte[] encodeObject(final Object obj) throws IOException {
        ByteArrayOutputStream baos = null;
        HessianOutput output = null;
        try {
            baos = new ByteArrayOutputStream(1024);
            output = new HessianOutput(baos);
            output.startCall();
            output.writeObject(obj);
            output.completeCall();
        }
        catch (final IOException ex) {
            throw ex;
        }
        finally {
            if (output != null) {
                try {
                    baos.close();
                }
                catch (final IOException ex) {
                    this.logger.error("Failed to close stream.", ex);
                }
            }
        }
        return baos != null ? baos.toByteArray() : null;
    }

}
