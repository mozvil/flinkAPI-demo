package com.mozvil.time;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OtherWindowDemo {

	/**
	 * 其他类型窗口指派
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		
		// id,eventId,timestamp,duration
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		
		// 将String数据映射成EventBean对象
		SingleOutputStreamOperator<EventBean2> beanStream = sourceStream.map(s -> {
			String[] arr = s.split(",");
			return new EventBean2(Long.valueOf(arr[0]), arr[1], Long.valueOf(arr[2]), arr[3], Integer.valueOf(arr[4]));
		}).returns(EventBean2.class)
			.assignTimestampsAndWatermarks(
					WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ofMillis(0))
						.withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
							private static final long serialVersionUID = 1L;
							
							@Override
							public long extractTimestamp(EventBean2 element, long recordTimestamp) {
								return element.getTimestamp();
							}
							
						})
			);
		
		/**
		 * 以下为各种类型窗口的开启API
		 */
		/*
		// 全局计数滚动窗口 参数：窗口长度为10条记录
		beanStream.countWindowAll(10)
			.apply(new AllWindowFunction<EventBean2, String, GlobalWindow>() {

				private static final long serialVersionUID = -2300291301528456764L;

				@Override
				public void apply(GlobalWindow window, Iterable<EventBean2> values, Collector<String> out)
						throws Exception {
					// TODO Auto-generated method stub
					
				}
				
			});
		
		// 全局计数滑动窗口 参数：窗口长度为10条记录 向前滑动2条记录
		beanStream.countWindowAll(10, 2);
			//.apply() 后面接聚合算子
		
		// 全局事件时间滚动窗口 参数：窗口时长为30秒
		beanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
			.apply(new AllWindowFunction<EventBean2, String, TimeWindow>() {

				private static final long serialVersionUID = 1L;

				@Override
				public void apply(TimeWindow window, Iterable<EventBean2> values, Collector<String> out) throws Exception {
					// TODO Auto-generated method stub
					
				}
				
			});
		
		// 全局事件时间滑动窗口 参数：窗口时长为30秒 向前滑动10秒
		beanStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)));
			//.apply() 后面接聚合算子
		
		// 全局事件时间session窗口 参数：时间间隔超过30秒将划分一个新窗口(即以时间间隔为窗口划分依据)
		beanStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30)));
		//.apply() 后面接聚合算子
		
		// 全局处理时间滚动窗口 参数：窗口时长为30秒
		beanStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));
		//.apply() 后面接聚合算子
		
		// 全局处理时间滑动窗口 参数：窗口时长为30秒 向前滑动10秒
		beanStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));
		//.apply() 后面接聚合算子
		
		// 全局处理时间session窗口 参数：时间间隔超过30秒将划分一个新窗口(即以时间间隔为窗口划分依据)
		beanStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));
		//.apply() 后面接聚合算子
		
		
		
		KeyedStream<EventBean2, Long> keyedStream = beanStream.keyBy(EventBean2::getGuid);
		// keyed计数滚动窗口 参数：窗口长度为10条记录
		keyedStream.countWindow(10);
		//.apply() 后面接聚合算子
		
		// keyed计数滑动窗口 参数：窗口长度为10条记录 向前滑动2条记录
		keyedStream.countWindow(10, 2);
		//.apply() 后面接聚合算子
		
		// keyed事件时间滚动窗口 参数：窗口时长为30秒
		keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(30)));
		
		// keyed事件时间滑动窗口 参数：窗口时长为30秒 向前滑动10秒
		keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)));
		
		// keyed事件时间session窗口 参数：时间间隔超过30秒将划分一个新窗口(即以时间间隔为窗口划分依据)
		keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(30)));
		//.apply() 后面接聚合算子
		
		// keyed处理时间滚动窗口 参数：窗口时长为30秒
		keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));
		//.apply() 后面接聚合算子
		
		// keyed处理时间滑动窗口 参数：窗口时长为30秒 向前滑动10秒
		keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));
		//.apply() 后面接聚合算子
		
		// keyed会话时间窗口 参数：时间间隔超过30秒将划分一个新窗口(即以时间间隔为窗口划分依据)
		keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)));
		//.apply() 后面接聚合算子
		*/
		
		env.execute();
	}

}
