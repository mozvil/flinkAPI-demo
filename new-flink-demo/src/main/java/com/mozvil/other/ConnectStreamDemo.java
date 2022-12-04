package com.mozvil.other;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStreamDemo {
	
	/**
	 * 两个流合并处理
	 * 原理：合并后产生一个新的流，对两个流做统一处理(一个处理方法对应一个流) 但处理过程中如有状态，则两个方法可以共享这个状态
	 * @param args
	 * @throws Exception 
	 */

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(2);
		
		// 一个数字字符流
		DataStreamSource<String> stream1 = env.socketTextStream("hadoop100", 9998);
		// 一个字母字符流
		DataStreamSource<String> stream2 = env.socketTextStream("hadoop100", 9999);
		
		// 合并流
		ConnectedStreams<String, String> connectedStreams = stream1.connect(stream2);
		// 这里如果需要用flatMap处理就传入一个CoFlatMapFunction的匿名类(泛型类型相同) 其中需要实现两个方法(flatMap1和flatMap2) 原理和Map相同
		SingleOutputStreamOperator<String> resultStream = connectedStreams.map(new CoMapFunction<String, String, String>() {

			private static final long serialVersionUID = -5002359734712564715L;
			
			String prefix = "mozvil-";
			
			/**
			 * 对stream1处理的逻辑
			 */
			@Override
			public String map1(String value) throws Exception {
				return prefix + (Integer.parseInt(value) * 10);
			}

			/**
			 * 对stream2处理的逻辑
			 */
			@Override
			public String map2(String value) throws Exception {
				return prefix + value.toUpperCase();
			}
			
		});
		
		stream1.print("stream1(number)");
		stream2.print("stream2(string)");
		resultStream.print("connected stream");
		
		env.execute();
	}

}
