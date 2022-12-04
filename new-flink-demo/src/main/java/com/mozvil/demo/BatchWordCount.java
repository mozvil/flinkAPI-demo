package com.mozvil.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ExecutionEnvironment是flink的批计算执行环境，它使用的是DataSet的一套API
 * @author Jeff Home
 *
 */
//从Flink 1.12开始官方推荐直接使用DataStream API，不再用DataSet API(软弃用)了
public class BatchWordCount {
	
	public static void main(String[] args) throws Exception {
		// 1. 创建执行环境
		//ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// 本地开启flink作业 使用createLocalEnvironmentWithWebUI方法创建流 可设置webui访问端口 localhost:8081
		// Windows打开Socket命令 nc -lvp 9999
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		
		// 2. 从文件读取数据(获取数据源)
		DataStreamSource<String> lineDataSource = env.socketTextStream("localhost", 9999);
		// 3. 将文件每行数据分词再转换成二元数组类型
		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
			// 将一行文本分词
			String[] words = line.split(" ");
			// 将每个单词转换成二元数组输出
			for(String word : words) {
				out.collect(Tuple2.of(word, 1));
			}
		}).returns(Types.TUPLE(Types.STRING, Types.INT));
		wordAndOneTuple.keyBy(data -> data.f0).sum(1).print();
		env.execute();
	}

}
