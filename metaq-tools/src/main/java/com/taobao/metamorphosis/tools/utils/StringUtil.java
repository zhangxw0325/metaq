package com.taobao.metamorphosis.tools.utils;

public class StringUtil {
	public static boolean empty(String s) {
		return s == null || s.length() == 0 || s.trim().length() == 0;
	}
}
