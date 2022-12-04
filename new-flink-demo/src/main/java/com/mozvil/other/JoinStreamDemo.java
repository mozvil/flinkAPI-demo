package com.mozvil.other;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class JoinStreamDemo {

	/**
	 * Join和CoGroup类似 它是CoGroup的一种封装
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(2);
		
		// id, name
		DataStreamSource<String> stream1 = env.socketTextStream("hadoop100", 9998);
		// id, age, city
		DataStreamSource<String> stream2 = env.socketTextStream("hadoop100", 9999);
		
		// 将两个string类型的输入流分别转成一个二元组类型和一个三元组类型的流
		SingleOutputStreamOperator<Tuple2<String, String>> tStream1 = stream1.map(s -> {
			String[] arr = s.split(",");
			return Tuple2.of(arr[0], arr[1]);
		}).returns(new TypeHint<Tuple2<String, String>>(){});
		SingleOutputStreamOperator<Tuple3<String, String, String>> tStream2 = stream2.map(s -> {
			String[] arr = s.split(",");
			return Tuple3.of(arr[0], arr[1], arr[2]);
		}).returns(new TypeHint<Tuple3<String, String, String>>(){});
		
		// Join操作(Join逻辑为两个流数据中的id字段相等)
		// 相当于SQL中的inner操作(在15秒的窗口期内的所有数据按where和equalTo中的字段匹配 所有匹配成功的数据都会进入到join方法 匹配不成功的则不会进入join)
		DataStream<String> resultStream = tStream1.join(tStream2)
				.where(tp -> tp.f0)
				.equalTo(tp -> tp.f0)
				// 时间窗口(15m秒)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
				.apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

					private static final long serialVersionUID = -6380654221568022490L;

					@Override
					public String join(Tuple2<String, String> first, Tuple3<String, String, String> second) throws Exception {
						return first.f0 + ", " + first.f1 + ", " + second.f0 + ", " + second.f1 + ", " + second.f2;
					}
					
				});
		
		resultStream.print();
		
		env.execute();
	}

}
