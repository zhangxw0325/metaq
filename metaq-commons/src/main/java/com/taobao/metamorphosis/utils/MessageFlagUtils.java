package com.taobao.metamorphosis.utils;

import com.taobao.metamorphosis.Message;


/**
 * 消息flag工具类 flag是32位整数，它的结构如下：</br></br>
 * <ul>
 * <li>低一位，1表示有消息属性，否则没有</li>
 * <li>其他暂时保留</li>
 * </ul>
 * 
 * @author boyan
 * @Date 2011-4-29
 * 
 */
public class MessageFlagUtils {

    public static int getFlag(final Message message) {
        int flag = 0;
        if (message != null && message.getAttribute() != null) {
            // 低一位设置为1
            flag = flag & 0xFFFFFFFE | 1;
        }
        return flag;
    }


    public static int getFlagCompress(final int flag, final boolean compress) {
        int resultFlag = flag;
        if (compress) {
            // 低二位设置为1
            resultFlag = resultFlag & 0xFFFFFFFD | 2;
        }
        return resultFlag;
    }


    public static boolean hasAttribute(final int flag) {
        return (flag & 0x1) == 1;
    }


    public static boolean isCompress(final int flag) {
        return (flag & 0x2) == 2;
    }

}
