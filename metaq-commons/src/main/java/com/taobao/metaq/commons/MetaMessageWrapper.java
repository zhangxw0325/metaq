/**
 * $Id: MetaMessageWrapper.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.commons;

/**
 * 消息包装类，在服务器和Consumer上使用
 */
public class MetaMessageWrapper {
    private final MetaMessage metaMessage;
    private final MetaMessageAnnotation metaMessageAnnotation;


    public MetaMessageWrapper(MetaMessage metaMessage, MetaMessageAnnotation metaMessageAnnotation) {
        this.metaMessage = metaMessage;
        this.metaMessageAnnotation = metaMessageAnnotation;
    }


    public MetaMessage getMetaMessage() {
        return metaMessage;
    }


    public MetaMessageAnnotation getMetaMessageAnnotation() {
        return metaMessageAnnotation;
    }
}
