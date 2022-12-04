package com.mozvil.time;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class AllowLatenessWindowDemo {
	
	/**
	 * 允许迟到数据时间窗口演示
	 * 数据：
	 *    1,e01,10000,10
     *    1,e02,11000,20
	 *    1,e02,12000,40
	 *    1,e03,13000,30
	 *    1,e03,20000,10
	 *    1,e01,21000,50
	 *    1,e04,22000,10
	 *    1,e06,28000,60
	 *    1,e07,30000,10
	 * 
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
		
		// 将String数据映射成EventBean对象并封装成一个Tuple2(第二个元素为1 为方便计数)
		SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> beanStream = sourceStream.map(s -> {
			String[] arr = s.split(",");
			EventBean2 bean = new EventBean2(Long.valueOf(arr[0]), arr[1], Long.valueOf(arr[2]), arr[3], Integer.valueOf(arr[4]));
			return Tuple2.of(bean, 1);
		}).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {})
		.assignTimestampsAndWatermarks(
			WatermarkStrategy.<Tuple2<EventBean2, Integer>>forBoundedOutOfOrderness(Duration.ofMillis(0))
				.withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<EventBean2, Integer>>() {
					private static final long serialVersionUID = 1L;
					
					@Override
					public long extractTimestamp(Tuple2<EventBean2, Integer> element, long recordTimestamp) {
						return element.f0.getTimestamp();
					}
					
				})
		);
		
		// 定义一个测流输出Tag对象 用以将迟到超时的数据输出到测流中
		OutputTag<Tuple2<EventBean2, Integer>> lateDataTag = new OutputTag<Tuple2<EventBean2, Integer>>(
				"late_data",
				TypeInformation.of(new TypeHint<Tuple2<EventBean2, Integer>>() {})
		);
		
		SingleOutputStreamOperator<String> resultStream = beanStream
				.keyBy(tp -> tp.f0.getGuid())
				// 开启keyed事件时间滚动窗口统计10秒窗口中的数据条数
				.window(TumblingEventTimeWindows.of(Time.seconds(10)))
				// 设置一个自定义的Trigger(窗口触发)
				//.trigger(CustomEventTimeTrigger.create())
				// 设置一个自定义的Evictor(移除器)
				.evictor(new CustomTimeEvictor(1000 * 10, false, "e0x"))
				// 允许数据迟到2秒钟(将再次输出聚合统计结果)
				.allowedLateness(Time.seconds(2))
				// 迟到超出限制(2秒)的数据将被输出到测流中(主流将不再作聚合计算)
				.sideOutputLateData(lateDataTag)
				//.sum("f1");
				.apply(new WindowFunction<Tuple2<EventBean2, Integer>, String, Long, TimeWindow>() {

					private static final long serialVersionUID = 6090463130625969439L;

					@Override
					public void apply(Long key, TimeWindow window, 
							Iterable<Tuple2<EventBean2, Integer>> input,
							Collector<String> out) throws Exception {
						int count = 0;
						for(Tuple2<EventBean2, Integer> tuple : input) {
							count++;
						}
						out.collect("[" + window.getStart() + " - " + window.getEnd() + "]: " + count);
					}
					
				});
				
		
		// 获取测流中的所有迟到数据
		DataStream<Tuple2<EventBean2, Integer>> lateDataSideStream = resultStream.getSideOutput(lateDataTag);
		
		// 当1,e04,22000,10这条数据输入后Watermark将被推进到22秒 此时已超出窗口设置的允许数据迟到的时间
		// 那么如果此时还有10-20秒窗口的数据输入则会被输出到测流中
		resultStream.print("From main stream: ");
		lateDataSideStream.print("From side stream");
		
		env.execute();

	}

}
