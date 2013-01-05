package com.taobao.metamorphosis.utils.codec.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.caucho.hessian.io.HessianInput;
import com.taobao.metamorphosis.utils.codec.Deserializer;


/**
 * 
 * @author wuxin
 * @since 1.0, 2009-10-20 ÉÏÎç10:03:15
 */
public class Hessian1Deserializer implements Deserializer {

    private final Logger logger = Logger.getLogger(Hessian1Deserializer.class);


    /**
     * @see com.taobao.notify.codec.Deserializer#decodeObject(byte[])
     */
    @Override
    public Object decodeObject(final byte[] in) throws IOException {
        Object obj = null;
        ByteArrayInputStream bais = null;
        HessianInput input = null;
        try {
            bais = new ByteArrayInputStream(in);
            input = new HessianInput(bais);
            input.startReply();
            obj = input.readObject();
            input.completeReply();
        }
        catch (final IOException ex) {
            throw ex;
        }
        catch (final Throwable e) {
            this.logger.error("Failed to decode object.", e);
        }
        finally {
            if (input != null) {
                try {
                    bais.close();
                }
                catch (final IOException ex) {
                    this.logger.error("Failed to close stream.", ex);
                }
            }
        }
        return obj;
    }
}
