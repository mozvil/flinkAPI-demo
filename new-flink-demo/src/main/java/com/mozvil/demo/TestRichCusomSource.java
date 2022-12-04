package com.mozvil.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义Source继承RichParallelSourceFunction 添加此source可以设置大于1的并行度
 * 自定义Source继承RichSourceFunction 添加此source不能设置大于1的并行度(因为它是单并行度SourceFunction, 如果设置大于1的并行度程序将报错)
 */
public class TestRichCusomSource extends RichParallelSourceFunction<Event> {

	private static final long serialVersionUID = -4261786460816792481L;

	volatile boolean flag = true;

	@Override
	public void open(Configuration parameters) throws Exception {
		// Rich算子可以获取运行时上下文
		RuntimeContext runtimeContext = getRuntimeContext();
		// 运行时上下文可以获取算子名和算子实例的ID等
		String taskName = runtimeContext.getTaskName();
		int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
		System.out.println("----------------Task Begin--------------");
		System.out.println("Task Name: " + taskName);
		System.out.println("Subtask ID: " + indexOfThisSubtask);
	}

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
			Thread.sleep(RandomUtils.nextInt(1000, 5000));
		}
	}

	@Override
	public void cancel() {
		flag = false;
	}
	
	@Override
	public void close() throws Exception {
		System.out.println("----------------Task has finished & closed----------------");
	}

}
