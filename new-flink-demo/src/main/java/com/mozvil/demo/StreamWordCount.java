package com.mozvil.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {

	public static void main(String[] args) throws Exception {
		// 1. 创建流式执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setParallelism(1);
		// 使用流式执行环境可以开启批计算的运行模式
		// 当开启批计算的运行模式输出结果是聚合后的最终结果
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		
		// 2. 读取文件
		DataStreamSource<String> fileSource = env.readTextFile("D:/words.txt");
		
		// 3. 将文件每行数据分词再转换成二元数组类型
		SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = fileSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
			// 将一行文本分词
			String[] words = line.split(" ");
			// 将每个单词转换成二元数组输出
			for(String word : words) {
				out.collect(Tuple2.of(word, 1L));
			}
		}).returns(Types.TUPLE(Types.STRING, Types.LONG));
		
		// 4. 分组
		KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);
		
		// 5. 求和
		SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);
		
		sum.print();
		
		env.execute();
	}

}
