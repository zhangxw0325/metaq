/**
 * $Id: DefaultMessageFilter.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

import java.util.Set;


/**
 * ÏûÏ¢¹ıÂË
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class DefaultMessageFilter implements MessageFilter {
	static final int defaultHashCode = "*".hashCode();

    public boolean isMessageMatched(Set<Integer> types, int type) {
        if (null == types || types.isEmpty() || types.contains(defaultHashCode))
            return true;
        return types.contains(type);
    }
}
