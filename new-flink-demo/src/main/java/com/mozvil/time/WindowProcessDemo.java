package com.mozvil.time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowProcessDemo {

	/**
	 * 需求：每隔10秒统计最近30秒内数据中每个id的平均duration
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		
		// id,eventId,timestamp,pageId,duration
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		
		// 将String数据映射成EventBean对象(为了获取时间戳)
		SingleOutputStreamOperator<EventBean2> beanStream = sourceStream.map(s -> {
			String[] arr = s.split(",");
			return new EventBean2(Long.valueOf(arr[0]), arr[1], Long.valueOf(arr[2]), arr[3], Integer.valueOf(arr[4]));
		}).returns(EventBean2.class);
		
		// 分配Watermark到beanStream上(源头)
		SingleOutputStreamOperator<EventBean2> waterMarkedBeanStream = beanStream.assignTimestampsAndWatermarks(
				// 不允许乱序(单调递增)的算法策略
				WatermarkStrategy.<EventBean2>forBoundedOutOfOrderness(Duration.ofMillis(0))
				// 时间戳抽取方法(从数据中获取时间戳的方法)
					.withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {

						private static final long serialVersionUID = 1L;
	
						@Override
						public long extractTimestamp(EventBean2 element, long recordTimestamp) {
							return element.getTimestamp();
						}
						
					})
		);
		
		// 概念：
		//     1. 滚动聚合   - 在数据统计周期内(可能是时间窗口或条数窗口)每当有数据到达就发生一次聚合计算 直到窗口结束(触发窗口计算条件满足)输出一个最终的计算结果
		//     2. 全窗口聚合 - 在数据统计周期内(可能是时间窗口或条数窗口)将所有数据累计存放在内存中(可看作一个数据桶) 在窗口期内不做任何计算直到窗口结束(触发窗口计算条件满足)一次性计算得到结果并输出
		// 滚动聚合的效率更高因为它每次计算完数据不用放入内存 全窗口聚合的意义是在计算过程中可以做到不止聚合的简单逻辑(可以添加其他逻辑)更为灵活 具体看业务需求
		
		// 按id keyBy数据
		
		// 需求1：每隔10秒统计最近30秒内数据中每个id的平均duration
		/*
		SingleOutputStreamOperator<Double> resultStream = waterMarkedBeanStream
			.keyBy(EventBean2::getGuid)
			// 开启窗口#1 参数：窗口长度(30秒)  滑动步长(10秒)
			.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
			// 聚合算法(滚动聚合)AggregateFunction泛型类型：
			//    1. 进入数据类型
			//    2. 聚合状态(滚动聚合每次计算得到的最新值) 此处用一个二元组保存 二元组第一个元素保存当前累计数据条数 第二个元素保存累计的duration
			//    3. 输出结果类型 窗口结束后返回聚合状态保存的二元组中累计duration除以累计数据条数得到一个平均值
			.aggregate(new AggregateFunction<EventBean2, Tuple2<Integer, Integer>, Double>() {

				private static final long serialVersionUID = 1L;
				
				//累计状态的初始化方法
				@Override
				public Tuple2<Integer, Integer> createAccumulator() {
					return Tuple2.of(0, 0);
				}

				// 累计状态的更新逻辑 value 进入的数据  accumulator 更新后的累计状态
				@Override
				public Tuple2<Integer, Integer> add(EventBean2 value, Tuple2<Integer, Integer> accumulator) {
					return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value.getDuration());
				}

				// 输出聚合最后的结果 accumulator 累计状态
				@Override
				public Double getResult(Tuple2<Integer, Integer> accumulator) {
					return (double)accumulator.f1 / accumulator.f0;
				}

				// 批计算中shuffle的上游可以做局部聚合 然后将局部聚合后结果交给下游做全局聚合
				// 需要提供两个局部聚合结果机型合并的逻辑
				// 流式计算中不需要这个逻辑且不用实现
				@Override
				public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
					return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
				}
				
			});
		resultStream.print();
		*/
		
		// 需求2.1 每隔10秒统计最近30秒内数据中每个id的duration时长最长的两条记录(使用全窗口聚合的方法)
		/*
		SingleOutputStreamOperator<EventBean2> resultStream21 = waterMarkedBeanStream
				.keyBy(EventBean2::getGuid)
				// 开启窗口#1 参数：窗口长度(30秒)  滑动步长(10秒 每10秒统计一次) 即窗口范围：-10秒 ~ 20秒
				.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
				// apply算子参数为WindowFunction(继承自Function)泛型类型：
				//    泛型1 - 输入数据类型(EventBean2)
				//    泛型2 - 输出聚合计算结果类型(使用apply方法可以自己决定输出的数据类型和条数 自由度较高)
				//    泛型3 - key的类型
				//    泛型4 - 输入每个Key值的窗口类型(在分组情况下Flink会为每个Key值的每个时间窗口开辟一个桶来存放数据)
				//           如：id为1的前30秒内的数据会放在一个桶内 id为2的前30秒内的数据则放入另一个桶内 这样可以将id不同的数据分隔开
				.apply(new WindowFunction<EventBean2, EventBean2, Long, TimeWindow>() {

					private static final long serialVersionUID = 1L;
					
					// key - 这里要做聚合的窗口的key值(guid)
					// window - 本次窗口的元信息 如窗口起始结束时间
					// input - 本次窗口桶里的所有数据
					// output - 聚合计算数据输出
					@Override
					public void apply(
							Long key, 
							TimeWindow window, 
							Iterable<EventBean2> input,
							Collector<EventBean2> out) throws Exception {
						EventBean2 eb1 = null;
						EventBean2 eb2 = null;
						for(EventBean2 eb : input) {
							if(eb1 == null) {
								eb1 = eb;
								continue;
							}
							if(eb2 == null) {
								if(eb.getDuration() >= eb1.getDuration()) {
									eb2 = eb1;
									eb1 = eb;
									continue;
								} else {
									eb2 = eb;
									continue;
								}
							}
							if(eb.getDuration() >= eb1.getDuration()) {
								eb2 = eb1;
								eb1 = eb;
								continue;
							}
							if(eb.getDuration() >= eb2.getDuration()) {
								eb2 = eb;
								continue;
							}
						}
						if(eb1 != null) {
							out.collect(eb1);
						}
						if(eb2 != null) {
							out.collect(eb2);
						}
					}
					
				});
		resultStream21.print();
		*/
		
		// 需求2.2 每隔10秒统计最近30秒内数据中每个id的duration时长最长的两条记录(使用全窗口聚合的方法)
		SingleOutputStreamOperator<String> resultStream22 = waterMarkedBeanStream
				.keyBy(data -> data.getGuid())
				// 开启窗口#1 参数：窗口长度(30秒)  滑动步长(10秒 每10秒统计一次) 即窗口范围：-10秒 ~ 20秒
				.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
				// 也可以使用process算子来作聚合计算(本次输出String类型结果) process算子的自由度比apply更高
				// 原因是它的参数ProcessWindowFunction继承自AbstractRichFunction(RichFunction)
				.process(new ProcessWindowFunction<EventBean2, String, Long, TimeWindow>() {

					private static final long serialVersionUID = 2811477939265003820L;
					
					// key - 这里要做聚合的窗口的key值(guid)
					// context - 上下文环境(其中包含类窗口的元信息 与apply算子类似)
					// data - 本次窗口桶里的所有数据
					// out - 聚合计算数据输出
					@Override
					public void process(Long key,
							ProcessWindowFunction<EventBean2, String, Long, TimeWindow>.Context context,
							Iterable<EventBean2> data, Collector<String> out) throws Exception {
						TimeWindow timeWindow = context.window();
						// 窗口允许的最大时间戳(如 1000 - 1999 则最大的时间戳为1999)
						//long maxTimestamp = timeWindow.maxTimestamp();
						// 获取窗口的开始结束时间戳
						long windowStart = timeWindow.getStart();
						Long windowEnd = timeWindow.getEnd();
						
						EventBean2 eb1 = null;
						EventBean2 eb2 = null;
						for(EventBean2 eb : data) {
							if(eb1 == null) {
								eb1 = eb;
								continue;
							}
							if(eb2 == null) {
								if(eb.getDuration() >= eb1.getDuration()) {
									eb2 = eb1;
									eb1 = eb;
									continue;
								} else {
									eb2 = eb;
									continue;
								}
							}
							if(eb.getDuration() >= eb1.getDuration()) {
								eb2 = eb1;
								eb1 = eb;
								continue;
							}
							if(eb.getDuration() >= eb2.getDuration()) {
								eb2 = eb;
								continue;
							}
						}
						if(eb1 != null) {
							out.collect(windowStart + "-" + windowEnd + "Duration #1: [" + eb1.toString() + "]");
						}
						if(eb2 != null) {
							out.collect(windowStart + "-" + windowEnd + "Duration #2: [" + eb2.toString() + "]");
						}
						
					}
					
				});
		resultStream22.print();
		
		// 需求3：每隔10秒统计最近30秒的数据中每个guid的EventBean2记录条数(使用sum算子实现)
		/*
		waterMarkedBeanStream
				.map(bean -> Tuple2.of(bean, 1)).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {})
				.keyBy(tp -> tp.f0.getGuid())
				.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
				.sum("f1")
				.print();
		*/
		
		// 需求4.1：每隔10秒统计最近30秒的数据中每个guid的EventBean2记录中最大的duration(使用max算子实现)
		/*
		waterMarkedBeanStream
				.keyBy(EventBean2::getGuid)
				.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
				.max("duration")
				.print();
		*/
		
		// 需求4.2：每隔10秒统计最近30秒的数据中每个guid的EventBean2记录中最大的duration所在的一条记录(使用maxBy算子实现)
		/*
		waterMarkedBeanStream
				.keyBy(EventBean2::getGuid)
				.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
				.maxBy("duration")
				.print();
		*/
		
		// 需求5： 每隔10秒统计最近30秒的数据中每个pageId的event平均duration最大的2个eventId及其平均duration
		waterMarkedBeanStream
				.keyBy(bean -> bean.getGuid())
				.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
				.process(new ProcessWindowFunction<EventBean2, Tuple3<Long, String, Double>, Long, TimeWindow>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void process(
							Long key,
							ProcessWindowFunction<EventBean2, Tuple3<Long, String, Double>, Long, TimeWindow>.Context context,
							Iterable<EventBean2> elements, 
							Collector<Tuple3<Long, String, Double>> out) throws Exception {
						Map<String, Tuple2<Integer, Long>> tmpMap = new HashMap<String, Tuple2<Integer, Long>>();
						Tuple2<Integer, Long> tuple;
						for(EventBean2 element : elements) {
							tuple = tmpMap.getOrDefault(element.getEventId(), Tuple2.of(0, 0L));
							tmpMap.put(element.getEventId(), Tuple2.of(tuple.f0 + 1, tuple.f1 + element.getDuration()));
						}
						List<Tuple2<String, Double>> tmpList = new ArrayList<Tuple2<String, Double>>();
						for(Map.Entry<String, Tuple2<Integer, Long>> entry : tmpMap.entrySet()) {
							tmpList.add(Tuple2.of(entry.getKey(), (double)entry.getValue().f1 / entry.getValue().f0));
						}
						// 排序
						Collections.sort(tmpList, new Comparator<Tuple2<String, Double>>() {

							@Override
							public int compare(Tuple2<String, Double> tp1, Tuple2<String, Double> tp2) {
								//return tp2.f1.compareTo(tp1.f1);
								return Double.compare(tp2.f1, tp1.f1);
							}
							
						});
						// 输出前两条记录
						for(int i=0; i<Math.min(tmpList.size(), 2); i++) {
							out.collect(Tuple3.of(key, tmpList.get(i).f0, tmpList.get(i).f1));
						}
					}
					
				}).print();
		
		env.execute();
	}

}
