package com.taobao.metamorphosis.client.consumer;

import com.taobao.metamorphosis.exception.UnknowCodecTypeException;
import com.taobao.metamorphosis.utils.codec.Deserializer;
import com.taobao.metamorphosis.utils.codec.Serializer;
import com.taobao.metamorphosis.utils.codec.impl.Hessian1Deserializer;
import com.taobao.metamorphosis.utils.codec.impl.Hessian1Serializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaDeserializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaSerializer;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-10-31 ÏÂÎç5:42:50
 */

public abstract class AbstractRecoverManager implements RecoverManager {

    private final String META_RECOVER_CODEC_TYPE = System.getProperty("meta.recover.codec", "java");
    protected final Serializer serializer;
    protected final Deserializer deserializer;


    public AbstractRecoverManager() {
        if (this.META_RECOVER_CODEC_TYPE.equals("java")) {
            this.serializer = new JavaSerializer();
            this.deserializer = new JavaDeserializer();
        }
        else if (this.META_RECOVER_CODEC_TYPE.equals("hessian1")) {
            this.serializer = new Hessian1Serializer();
            this.deserializer = new Hessian1Deserializer();
        }
        else {
            throw new UnknowCodecTypeException(this.META_RECOVER_CODEC_TYPE);
        }
    }
}
