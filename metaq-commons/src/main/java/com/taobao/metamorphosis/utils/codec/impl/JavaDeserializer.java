package com.taobao.metamorphosis.utils.codec.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.log4j.Logger;

import com.taobao.metamorphosis.utils.codec.Deserializer;


/**
 * 
 * @author wuxin
 * @since 1.0, 2009-10-20 ÉÏÎç09:47:11
 */
public class JavaDeserializer implements Deserializer {

    private final Logger logger = Logger.getLogger(JavaDeserializer.class);


    /**
     * @see com.taobao.notify.codec.Deserializer#decodeObject(byte[])
     */
    @Override
    public Object decodeObject(final byte[] objContent) throws IOException {
        Object obj = null;
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;
        try {
            bais = new ByteArrayInputStream(objContent);
            ois = new ObjectInputStream(bais);
            obj = ois.readObject();
        }
        catch (final IOException ex) {
            throw ex;
        }
        catch (final ClassNotFoundException ex) {
            this.logger.warn("Failed to decode object.", ex);
        }
        finally {
            if (ois != null) {
                try {
                    ois.close();
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
