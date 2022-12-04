package com.mozvil.time;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class WatermarkDemo {
	
	/**
	 * Watermark是Flink用于推进时间维度的一种机制(一个标记)，它被封装成一个Bean(一个时间戳成员变量)放在用户数据流中从上游算子往下游算子中传输
	 * 默认Watermark每200毫秒(API可修改)就会传输一次(传输前会检查一下算子中的时间是否需要变更)，下游算子根据接收到的Watermark来跟新自己的时间标记
	 * Watermark被用于开启统计时间窗口和定时任务，算子根据Watermark更新的时间来触发时间窗口的统计以及启动定时任务的
	 * 一般来说Watermark是在source算子中(由开发人员决定 也可以在其他算子中)形成往下游定时发送的
	 * Watermark的起始时间可以从形成点(源头)算子通过API来指定数据中的某个字段
	 * Watermark的idle机制：可以通过API在Watermark的源头设置一个idle时长 当源头算子超过这个时长未接收到任何数据源头会主动更新Watermark(加上指定时间戳)发往下游
	 * 可在源头通过API设置一个容错时长来容纳迟到的数据(如果没有容错的时长 迟到数据将被抛弃) 容错只能缓解数据乱序的问题
	 * @throws Exception 
	 */

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);

		// id,event,timestamp,XXX
		DataStreamSource<String> stream = env.socketTextStream("hadoop100", 9998);
		// 在source算子中添加Watermark生成策略
		// Flink内置有3中生成策略：
		//     策略#1 - WatermarkStrategy.noWatermarks() 不生成Watermark(即禁用了事件事件的推进机制)
		//     策略#2 - WatermarkStrategy.forMonotonousTimestamps() 紧跟最大事件事件(没有容错事件)
		//     策略#3 - WatermarkStrategy.forBoundedOutOfOrderness() 允许乱序的Watermark生成策略
		//             (需要传入一个事件片Duration参数表示容错的时长 如传入时长为0 则效果与上述策略#2相同)
		// WatermarkStrategy.forGenerator() 自定义watermark生成算法(很少使用)
		
		// 示例1：在source(源头算子上生成Watermark
		// 构造Watermark的生成策略对象
		/*
		WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
				// 设置允许乱序的算法策略
				.<String>forBoundedOutOfOrderness(Duration.ofMillis(2000L))
				// 时间戳抽取方法(从数据中获取时间戳的方法)
				.withTimestampAssigner((data, timestamp) -> Long.valueOf(data.split(",")[2]));
		*/
		// 将上述Watermark的生成策略对象分配给source算子
		//stream.assignTimestampsAndWatermarks(watermarkStrategy);
		
		// 示例2：在一个中间算子上生成Watermark
		// map算子将source中的String数据转化成EventBean对象
		SingleOutputStreamOperator<EventBean> beanStream = stream.map(s -> {
			String[] arr = s.split(",");
			return new EventBean(Long.valueOf(arr[0]), arr[1], Long.valueOf(arr[2]), arr[3]);
		}).returns(EventBean.class)
		// 构造Watermark的生成策略对象
				.assignTimestampsAndWatermarks(
						// 不允许乱序(单调递增)的算法策略
						WatermarkStrategy.<EventBean>forMonotonousTimestamps()
						// 时间戳抽取方法(从数据中获取时间戳的方法)
						.withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {

							private static final long serialVersionUID = 1L;

							@Override
							public long extractTimestamp(EventBean element, long recordTimestamp) {
								return element.getTimestamp();
							}
							
						})
				);
		
		// 在下游算子中打印处理时间、Watermark		
		beanStream.process(new ProcessFunction<EventBean, EventBean>() {

			private static final long serialVersionUID = -7321424201884744941L;

			@Override
			public void processElement(
					EventBean data, 
					ProcessFunction<EventBean, EventBean>.Context context,
					Collector<EventBean> out) throws Exception {
				// 通过context.timeService()可获取到系统处理时间和watermark时间
				long processTime = context.timerService().currentProcessingTime();
				long watermark = context.timerService().currentWatermark();
				
				System.out.println("------------Recieved Data: " + data);
				System.out.println("------------Current Watermark: " + watermark);
				System.out.println("------------Current Process Time: " + processTime);
				out.collect(data);
			}
			
		}).print();
		
		env.execute();
	}

}
