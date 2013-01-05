package com.taobao.metamorphosis.tools.monitor.offsetcompareprob;

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.tools.domain.Group;

public class OffsetCompareProberTest {

	@Test
	public void testFindAlertList(){
		OffsetCompareProber ocp = new OffsetCompareProber(null);
		List<String> alertList = new LinkedList<String>();
		alertList.add("defaultWW");
		
		List<String> wwList = new LinkedList<String>();
		wwList.add("defaultWW");
		wwList.add("groupWW");
		
		List<String> topicList = new LinkedList<String>();
		topicList.add("groupTopic");
		
		List<String> ignoreTopicList = new LinkedList<String>();
		ignoreTopicList.add("groupTopicIgnore");
		
		Group group = new Group();
		group.setGroup("group1");
		group.setIgnoreTopicList(ignoreTopicList);
		group.setTopicList(topicList);
		group.setWwList(wwList);
		List<Group> groupList = new LinkedList<Group>();
		groupList.add(group);
		
		
		ocp.findAlertList(groupList, "ww", "topic1", "group1", alertList);
		Assert.assertEquals("[defaultWW]", alertList.toString());
		
		ocp.findAlertList(groupList, "ww", "groupTopic", "group1", alertList);
		Assert.assertEquals("[defaultWW, groupWW]", alertList.toString());
		
		ocp.findAlertList(groupList, "ww", "groupTopicIgnore", "group1", alertList);
		Assert.assertEquals("[]", alertList.toString());
	}
}
