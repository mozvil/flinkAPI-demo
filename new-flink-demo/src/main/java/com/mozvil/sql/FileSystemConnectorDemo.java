package com.mozvil.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FileSystemConnectorDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		tableEnv.executeSql(
				"create table fs_table (" +
				"    user_id string, " +
				"    order_amount double, " +
				"    dt string, " +
				"    `hour` string" + 
				") partitioned by (dt, `hour`) with (" + // 根据dt和hour字段动态分区
				"    'connector' = 'filesystem', " +
				"    'path' = 'file:///d:/flinkSink/filetable/', " +
				"    'format' = 'json', " +
				"    'sink.partition-commit.delay' = '1 h', " +
				"    'sink.partition-commit.policy.kind' = 'success-file', " +
				"    'sink.rolling-policy.file-size' = '8M', " +
				"    'sink.rolling-policy.rollover-interval' = '30 min', " +
				"    'sink.rolling-policy.check-interval' = '10 second'" +
				")"
		);
		
		// 从socket读取数据转成一个Tuple4的数据流
		SingleOutputStreamOperator<Tuple4<String, Double, String, String>> stream = 
				env.socketTextStream("hadoop100", 9998)
				   .map(s -> {
					   String[] split = s.split(",");
					   return Tuple4.of(split[0], Double.parseDouble(split[1]), split[2], split[3]);
				   }).returns(new TypeHint<Tuple4<String, Double, String, String>>() {});
		
		tableEnv.createTemporaryView("t_orders", stream);
		tableEnv.executeSql("insert into fs_table select * from t_orders");
		
		/**
		 * 测试数据(9998 socket)：
		 * u01,88.8,2022-11-30,23
		 * u02,65.4,2022-12-01,00
		 * u03,69.2,2022-12-01,01
		 * u04,74.8,2022-12-01,02
		 * 查看路径D:/flinkSink/filetable下的数据文件
		 */
		
		env.execute();
	}

}
