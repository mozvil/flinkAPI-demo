package com.mozvil.state;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateOperatorApiDemo {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		// 开启状态托管Checkpointing
		// 参数1：快照获取时间间隔  参数2(可不传 默认就是EXACTLY_ONCE)：Checkpointing模式
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		// 开启Task故障重启 Flink有多种Task级别的重启机制默认为RestartStrategies.noRestart()(不会重启)
		// 使用RestartStrategies.fixedDelayRestart(固定延迟重启) 参数1：重启次数限制  参数2：每两次重启间隔时间
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
		
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		sourceStream
				.keyBy(s -> "0")
				.map(new RichMapFunction<String, String>() {

					private static final long serialVersionUID = -3079059601038654002L;
			
					private ListState<String> listState;
					private MapState<String, String> mapState;
					private ValueState<String> valueState;
					private ReducingState<Integer> reduceState;
					private AggregatingState<Integer, Double> aggregatingState;
			
					@Override
					public void open(Configuration parameters) throws Exception {
						RuntimeContext runtimeContext = getRuntimeContext();
						
						// 获取一个List结构的状态存储器
						listState = runtimeContext.getListState(new ListStateDescriptor<String>("list", String.class));
				
						// 获取一个Map结构的状态存储器
						mapState = runtimeContext.getMapState(new MapStateDescriptor<String, String>("xx", String.class, String.class));
						
						// 获取一个单值结构的状态存储器
						valueState = runtimeContext.getState(new ValueStateDescriptor<String>("vstate", String.class));
					
						// 获取一个Reduce聚合状态存储器
						// ReducingState用来存储每次聚合计算的结果 它的输入输出值为同一个类型(如聚合计算累计值 1 -> 1, 2 -> 3, 3 -> 6, 4 -> 10 ...)
						// ReducingStateDescriptor的第二个参数需要提供一个Reduce算法即ReduceFunction
						reduceState = runtimeContext.getReducingState(
								new ReducingStateDescriptor<Integer>(
										"reduceState",
										new ReduceFunction<Integer>() {

											private static final long serialVersionUID = -2110433715658752034L;
											
											/**
											 * value1是前一次聚合后的值
											 * value2是本次输入的值
											 * return 返回本次聚合计算后状态中的最新值
											 */
											@Override
											public Integer reduce(Integer value1, Integer value2) throws Exception {
												return value1 + value2;
											}
											
										},
										Integer.class
								)
						);
						
						// 获取一个aggregate聚合状态存储器
						// AggregateState用来存储每次聚合计算的结果 它的输入输出值可以不是同一个类型
						// 这里实现整数(Integer)插入返回平均值(Double)的聚合计算
						aggregatingState = runtimeContext.getAggregatingState(
								new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
										"aggState", 
										new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {

											private static final long serialVersionUID = 3179600915775245093L;

											@Override
											public Tuple2<Integer, Integer> createAccumulator() {
												return Tuple2.of(0, 0);
											}

											@Override
											public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
												return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value);
											}

											@Override
											public Double getResult(Tuple2<Integer, Integer> accumulator) {
												return (double)accumulator.f1 / accumulator.f0;
											}

											@Override
											public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
												return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
											}
											
										},
										TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})
								)
						);
					}

					@Override
					public String map(String value) throws Exception {
						/**
						 * listState的数据操作API
						 */
						// 获取整个listState的数据迭代器
						Iterable<String> stringList = listState.get();
						// 添加一个元素到listState中
						listState.add("a");
						// 一次性放入多个元素到listState中
						listState.addAll(Arrays.asList("a", "b", "c"));
						// 一次性将listState中的数据替换为传入list的所有元素
						listState.update(Arrays.asList("a1", "b1", "c1"));
						
						/**
						 * mapState的数据操作API
						 */
						// 从mapState中根据一个key获取一个value
						String v = mapState.get("a");
						// 判断mapState中是否包含指定的key
						boolean contain = mapState.contains("a");
						// 获取mapState的entry迭代器
						Iterator<Map.Entry<String, String>> entryIterator = mapState.iterator();
						// 获取mapState的entry Iterable(内含迭代器)
						Iterable<Map.Entry<String, String>> entryIterable = mapState.entries();
						// 在mapState中插入一个key/value对
						mapState.put("a", "100");
						// 判断mapState是否为空
						boolean isEmpty = mapState.isEmpty();
						// 一次性放入多个key/value对
						Map<String, String> dataMap = new HashMap<String, String>();
						dataMap.put("a", "1");
						dataMap.put("b", "2");
						mapState.putAll(dataMap);
						// 获取mapState中的所有key
						Iterable<String> keys = mapState.keys();
						// 获取mapState中的所有value
						Iterable<String> values = mapState.values();
						// 从mapState中移除key为"a"的条目
						mapState.remove("a");
						
						/**
						 * valueState(单值)的数据操作API
						 */
						// 更新
						valueState.update("xx");
						// 获取
						String str = valueState.value();
						
						/**
						 * reduceState的数据操作API
						 */
						// 向聚合状态中添加数据
						reduceState.add(10);
						// 获取聚合状态中的值
						Integer statValue = reduceState.get();
						
						/**
						 * aggregatingState的数据操作API
						 */
						// 向聚合状态中添加数据
						aggregatingState.add(10);
						// 获取聚合状态中的值
						Double avgDouble = aggregatingState.get();
						return null;
					}
			
				})
				.print();
	
		env.execute();
	}

}
