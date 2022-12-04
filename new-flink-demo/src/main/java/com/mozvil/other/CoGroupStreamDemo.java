package com.mozvil.other;

import org.apache.flink.api.common.functions.CoGroupFunction;
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
import org.apache.flink.util.Collector;

public class CoGroupStreamDemo {

	/**
	 * 协同分组Demo
	 * 测试数据：
	 *     stream1中输入数据
	 *       1,aa
	 *       2,bb
	 *       2,bbbb
	 *       3,cc
	 *     stream2中输入数据
	 *       1,18,beijing
	 *       1,22,shanghai
	 *       2,33,guangzhou
	 *       4,44,xian
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
		
		// 将转成二元组和三元组类型的流协同分组(分组逻辑为两个流数据中的id字段相等)
		DataStream<String> resultStream = tStream1.coGroup(tStream2)
			.where(tp -> tp.f0)
			.equalTo(tp -> tp.f0)
			// 时间窗口(15m秒)
			.window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
			.apply(new CoGroupFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {

				private static final long serialVersionUID = 6805535686198093024L;
				
				/**
				 * 协同分组的方法 
				 * 原理：
				 * 	   在上述时间窗口(15秒)内将来自两个流的数据根据where和equalTo中指定字段进行比对(类似SQL中的Join操作 选择两张表的不同字段进行比对)
				 *     将5秒内两个流的所有数据按上述条件比对成功的分到一组中并调用coGroup方法做处理后输出数据
				 *     比对不成功的也会有一个组 即两个迭代器中有一个是空的
				 * @param first 单组数据中来自第一个流的数据迭代器
				 * @param second 单组数据中来自第二个流的数据迭代器
				 * @param out 输出处理结果数据
				 * @throws Exception
				 */
				@Override
				public void coGroup(
						Iterable<Tuple2<String, String>> first,
						Iterable<Tuple3<String, String, String>> second, 
						Collector<String> out) throws Exception {
					// 实现一个left join 先遍历first(以first中的数据为主)
					// 最后应该输出5条数据
					for(Tuple2<String, String> t1 : first) {
						boolean flag = false;
						for(Tuple3<String, String, String> t2 : second) {
							out.collect(t1.f0 + ", " + t1.f1 + ", " + t2.f0 + ", " + t2.f1 + ", " + t2.f2);
							flag = true;
						}
						// 这一组数据中来自第二个流的数据迭代器为空 只输出第一个数据流中的数据
						if(!flag) {
							out.collect(t1.f0 + ", " + t1.f1 + ",  ,  , ");
						}
					}
				}
				
			});
		resultStream.print();
		env.execute();
	}

}
