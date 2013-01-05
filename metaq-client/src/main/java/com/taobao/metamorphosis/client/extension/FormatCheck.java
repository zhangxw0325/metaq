package com.taobao.metamorphosis.client.extension;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class FormatCheck 
{
	//topic format
	static final Pattern TOPIC_FORMAT=Pattern.compile("[\\w\\-]+");
	
	public static void checkTopicFormat(final String topic) {
    	Matcher matcher=TOPIC_FORMAT.matcher(topic);
    	if(!matcher.matches()){
    		throw new IllegalArgumentException("format error topic:" + topic);
    	}
    }  
}
