package com.mozvil.other;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroardCastStreamDemo {

	/**
	 * BroardCast广播流通常作为关联维度表以状态的形式提供给其他流
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(2);
		
		// id, event
		DataStreamSource<String> stream1 = env.socketTextStream("hadoop100", 9998);
		// id, age, city 广播流中的数据不会有重复id 且不确定什么时候会有数据
		DataStreamSource<String> stream2 = env.socketTextStream("hadoop100", 9999);
		
		// 将两个string类型的输入流分别转成一个二元组类型和一个三元组类型的流
		SingleOutputStreamOperator<Tuple2<String, String>> mainStream = stream1.map(s -> {
			String[] arr = s.split(",");
			return Tuple2.of(arr[0], arr[1]);
		}).returns(new TypeHint<Tuple2<String, String>>(){});
		SingleOutputStreamOperator<Tuple3<String, String, String>> userInfoStream = stream2.map(s -> {
			String[] arr = s.split(",");
			return Tuple3.of(arr[0], arr[1], arr[2]);
		}).returns(new TypeHint<Tuple3<String, String, String>>(){});
		
		// 把mainStream(用户事件流)中的数据加宽
		// 创建一个状态(由Flink管理的状态 有持久化)描述
		MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc = new MapStateDescriptor<String, Tuple2<String, String>>(
				"userInfoState",
				TypeInformation.of(String.class),
				TypeInformation.of(new TypeHint<Tuple2<String, String>>(){})
		);
		BroadcastStream<Tuple3<String, String, String>> broadCastStream = userInfoStream.broadcast(userInfoStateDesc);
		
		// 连接广播流
		BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connectedStream = mainStream.connect(broadCastStream);
		// process参数区别：
		//     如果connectedStream是一个KeyedStream则传入KeyedBroadcastProcessFunction
		//     如果不是则传入BroadcastProcessFunction
		SingleOutputStreamOperator<String> resultStream = connectedStream.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

			private static final long serialVersionUID = 5698957611809003179L;
			
			/**
			 * 处理主数据流
			 * @param 主流上的数据 id event
			 * @param context 上下文(这个上下文是只读的 为了防止接受广播的流篡改广播中的状态 
			 *                因为广播中的状态数据是被所有接受广播的数据流共享的 这里修改后会造成数据不一致 所以推荐使用只读的上下文来获取状态中的数据
			 * @param out 输出器
			 * @throws Exception
			 */
			@Override
			public void processElement(Tuple2<String, String> value,
					BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext context,
					Collector<String> out) throws Exception {
				// 通过上下文获取一个只读的广播数据状态
				ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = context.getBroadcastState(userInfoStateDesc);
				
				// 把数据加宽(注意需要判断状态是否为空或状态中的数据二元组是否获取不到也为空)
				if(broadcastState != null) {
					Tuple2<String, String> userInfo = broadcastState.get(value.f0);
					out.collect(value.f0 + ", " + value.f1 + ", " + (userInfo == null ? "null" : userInfo.f0) + ", " + (userInfo == null ? "null" : userInfo.f1));
				} else {
					out.collect(value.f0 + ", " + value.f1 + ", ,");
				}
				// 获取广播流状态(只读)
				//context.getBroadcastState()
			}
			
			/**
			 * 处理广播流
			 * @param 广播流上的数据 id age city
			 * @param context 上下文
			 * @param out 输出器
			 * @throws Exception
			 */
			@Override
			public void processBroadcastElement(
					Tuple3<String, String, String> value,
					BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context context,
					Collector<String> out) throws Exception {
				// 获取被Flink管理的状态
				BroadcastState<String, Tuple2<String, String>> broadcastState = context.getBroadcastState(userInfoStateDesc);
				// 将广播流里的数据放入状态中
				broadcastState.put(value.f0, Tuple2.of(value.f1, value.f2));
				// 获取广播流状态
				//context.getBroadcastState()
			}
			
		});
		
		resultStream.print();
		
		env.execute();

	}

}
