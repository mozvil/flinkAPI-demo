package com.mozvil.function;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction作为最底层的流处理算子具有最高的灵活度
 * 对应不同类型的流它有多个变种：
 *      KeyedProcessFunction
 *      ProcessWindwoFunction
 *      ProcessAllWindowFunction
 *      CoProcessFunction
 *      ProcessJoinFunction
 *      BroadcastProcessFunction
 *      KeyedBroadcastProcessFunction
 * @author Jeff Home
 *
 */
public class CustomProcessFunction {
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

		// id, event
		DataStreamSource<String> stream1 = env.socketTextStream("hadoop100", 9998);
		
		
		// 将两个string类型的输入流分别转成一个二元组类型和一个三元组类型的流
		/*
		SingleOutputStreamOperator<Tuple2<String, String>> mainStream = stream1.map(s -> {
			String[] arr = s.split(",");
			return Tuple2.of(arr[0], arr[1]);
		}).returns(new TypeHint<Tuple2<String, String>>(){});
		*/
		// 以下使用ProcessFunction处理stream1流(输出一个二元组)效果与上面的map算子是完全一样的
		SingleOutputStreamOperator<Tuple2<String, String>> mainStream = stream1.process(new ProcessFunction<String, Tuple2<String, String>>() {

			private static final long serialVersionUID = -3588304434753892472L;

			@Override
			public void processElement(String value, 
					ProcessFunction<String, Tuple2<String, String>>.Context context,
					Collector<Tuple2<String, String>> out) throws Exception {
				String[] arr = value.split(",");
				out.collect(Tuple2.of(arr[0], arr[1]));
			}

		});
		
		KeyedStream<Tuple2<String, String>, String> keyedStream = mainStream.keyBy(tp2 -> tp2.f0);
		// KeyedStream类型调用process时需要传入KeyedProcessFunction
		keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {

			private static final long serialVersionUID = -6948872490102678431L;

			@Override
			public void processElement(Tuple2<String, String> value,
					KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>.Context context,
					Collector<Tuple2<Integer, String>> out) throws Exception {
				// 将输入的二元组中的ID变成Integer 将eventId变成大写 输出
				out.collect(Tuple2.of(Integer.valueOf(value.f0), value.f1.toUpperCase()));
			}
			
		});
		
		
		// id, age, city 广播流中的数据不会有重复id 且不确定什么时候会有数据
		DataStreamSource<String> stream2 = env.socketTextStream("hadoop100", 9999);
		SingleOutputStreamOperator<Tuple3<String, String, String>> userInfoStream = stream2.map(s -> {
			String[] arr = s.split(",");
			return Tuple3.of(arr[0], arr[1], arr[2]);
		}).returns(new TypeHint<Tuple3<String, String, String>>(){});
		
		
	}

}
