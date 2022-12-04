package com.mozvil.other;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSON;
import com.mozvil.demo.Event;
import com.mozvil.demo.TestRichCusomSource;

public class SplitStreamDemo {
	
	/**
	 * 演示测流输出
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		// http://localhost:8081/
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);

		DataStreamSource<Event> dataStreamSource = env.addSource(new TestRichCusomSource());
		
		// 将行为事件分流：
		//    eventId为appLaunch的事件分到一个流上
		//    eventId为putBack的事件分到一个流上
		//    其他事件继续在主流上输出
		SingleOutputStreamOperator<Event> processedStream = dataStreamSource.process(new ProcessFunction<Event, Event>() {

			private static final long serialVersionUID = -2633419149744457263L;

			/**
			 * 参数说明：
			 *    参数#1 event 输入的数据
			 *    参数#2 context 上下文 它提供测流输出的功能
			 *    参数#3 out主流输出的收集器
			 */
			@Override
			public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> out) throws Exception {
				if(event.getEventId().equals("appLaunch")) {
					context.output(new OutputTag<Event>("launch", TypeInformation.of(Event.class)), event);
				} else if(event.getEventId().equals("putBack")) {
					context.output(new OutputTag<String>("back", TypeInformation.of(String.class)), JSON.toJSONString(event.getEventInfo()));
				} else {
					// 如果这里没有else分支，则所有数据最后都还是会在主流中输出，符合上述条件的进入测流(输出2到3次)
					out.collect(event);
				}
			}
			
		});
		
		// 获取launch测流
		DataStream<Event> launchStream = processedStream.getSideOutput(new OutputTag<Event>("launch", TypeInformation.of(Event.class)));
		// 获取back测流
		DataStream<String> backStream = processedStream.getSideOutput(new OutputTag<String>("back", TypeInformation.of(String.class)));
		
		// 测流输出
		launchStream.print("launch");
		backStream.print("back");
		// 主流输出
		processedStream.print("mainStream");
		
		env.execute();
	}

}
