package com.mozvil.other;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionStreamDemo {
	
	/**
	 * Union与Connect区别：
	 *     Union是将两个流完全合并(并未做处理)一个新流 Connect是将两个流处理后再合并成一个新流
	 *     Union要合并的两个流必须是类型相同的 Connect可以将两个类型不同的流处理后输出一个其他统一类型的流
	 * @param args
	 * @throws Exception 
	 */

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		env.setParallelism(1);
		
		DataStreamSource<Integer> oddStream = env.fromElements(1, 3, 5, 7, 9);
		DataStreamSource<Integer> evenStream = env.fromElements(2, 4, 6, 8, 10);
		
		// 将奇数流与偶数流合并成1个流
		DataStream<Integer> resultStream = oddStream.union(evenStream);
		resultStream.map(i -> i * 10).print("Unioned");
		
		env.execute();
	}

}
