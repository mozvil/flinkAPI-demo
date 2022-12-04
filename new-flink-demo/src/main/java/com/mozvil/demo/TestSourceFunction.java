package com.mozvil.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class TestSourceFunction implements SourceFunction<Event> {

	private static final long serialVersionUID = 9060216945908558796L;
	
	volatile boolean flag = true;
	
	/**
	 * 死循环不断生成新的Event对象(模拟数据流)
	 */
	@Override
	public void run(SourceContext<Event> ctx) throws Exception {
		Event event = new Event();
		String[] users = {"王丽丽", "卞国华", "叶宁", "冯吉超", "陆毅成", "陈继来", "黄巍青", "小王"};
		String[] events = {"appLaunch", "pageLoad", "adShow", "adClick", "itemShare", "itemCollect", "putBack", "wakeUp", "appClose"};
		Map<String, String> eventInfoMap = new HashMap<String, String>();
		while(true) {
			event.setUser(users[RandomUtils.nextInt(0, users.length)]);
			event.setEventId(events[RandomUtils.nextInt(0, events.length)]);
			event.setTimestamp(System.currentTimeMillis());
			eventInfoMap.put(RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(2));
			event.setEventInfo(eventInfoMap);
			ctx.collect(event);
			eventInfoMap.clear();
			Thread.sleep(RandomUtils.nextInt(500, 1500));
		}
	}

	@Override
	public void cancel() {
		flag = false;
	}

}
