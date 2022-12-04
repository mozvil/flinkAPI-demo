package com.mozvil.time;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class WatermarkMultiPerrallismDemo {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		
		// id,event,timestamp,XXX
		DataStreamSource<String> stream = env.socketTextStream("hadoop100", 9998);
		//stream.rebalance();
		
		// 示例3：在一个中间算子上生成Watermark 并且将这个算子的并行度设为2
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
				).setParallelism(2);
		
		
		// 将下游算子的并行度设为1并打印处理时间、Watermark
		// 这里打印出的Watermark时间是本次数据到达前的算子内记录的Watermark(还未更新)
		// 更新算子内的Watermark逻辑是：取最近接收到的上游传来的Watermark中最小的一个Watermark
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
			
		}).setParallelism(1).print();
		
		env.execute();

	}

}
