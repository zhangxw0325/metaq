package com.taobao.metamorphosis.tools.domain;

import java.util.List;

public class Group {
	private String group;
	private List<String> topicList;
	private List<String> wwList;
	private List<String> mobileList;
	private List<String> ignoreTopicList;
	
	public List<String> getIgnoreTopicList() {
		return ignoreTopicList;
	}
	public void setIgnoreTopicList(List<String> ignoreTopicList) {
		this.ignoreTopicList = ignoreTopicList;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public List<String> getWwList() {
		return wwList;
	}
	public void setWwList(List<String> wwList) {
		this.wwList = wwList;
	}
	public List<String> getMobileList() {
		return mobileList;
	}
	public void setMobileList(List<String> mobileList) {
		this.mobileList = mobileList;
	}
	public List<String> getTopicList() {
		return topicList;
	}
	public void setTopicList(List<String> topicList) {
		this.topicList = topicList;
	}
}
