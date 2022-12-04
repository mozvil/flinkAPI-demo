package com.mozvil.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class JDBCConnectorSinkDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		tableEnv.executeSql(
				"create table t_test_sink (" +
				"    id int primary key, " + // jdbc connector作为sink时如果需要做update/delete等操作需要指定主键(primary key)
				"    gender string, " +
				"    name string " + 				
				") with (" +
				"    'connector' = 'jdbc', " +
				"    'driver' = 'com.mysql.cj.jdbc.Driver', " +
				"    'url' = 'jdbc:mysql://192.168.20.130:3306/flink_test', " +
				"    'table-name' = 'stu2', " +
				"    'username' = 'root', " +
				"    'password' = 'QAZwsx12345678'" +
				")"
		);
		
		// 将两个Socket流中的数据做(stream1)left join
		// stream1中数据为：1,male  2,female
		DataStreamSource<String> stream1 = env.socketTextStream("hadoop100", 9998);
		// stream2中数据为：1,name1 2,name2
		DataStreamSource<String> stream2 = env.socketTextStream("hadoop100", 9999);
		
		// 将两个source流分别转成Bean1和Bean2类型的流
		SingleOutputStreamOperator<Bean1> bean1Stream = stream1.map(s -> {
			String[] arr = s.split(",");
			return new Bean1(Integer.parseInt(arr[0]), arr[1]);
		});
		SingleOutputStreamOperator<Bean2> bean2Stream = stream2.map(s -> {
			String[] arr = s.split(",");
			return new Bean2(Integer.parseInt(arr[0]), arr[1]);
		});
		
		// 将上面的Bean1和Bean2类型的数据流分别转成表
		tableEnv.createTemporaryView("bean1", bean1Stream);
		tableEnv.createTemporaryView("bean2", bean2Stream);
		
		tableEnv.executeSql(
				"insert into t_test_sink " + 
				"select bean1.id, bean1.gender, bean2.name " +
				"from bean1 " + 
				"    left join bean2 on bean1.id=bean2.id"
		);
		/**
		 * 测试流程：
		 * 1. 9998 socket中输入1,male  查看数据库得到 1   male   null
		 * 2. 9999 socket中输入1,jeff  查看数据库得到 1   male   jeff
		 * 3. 9999 socket中输入1,mozvil查看数据库得到 1   male   mozvil
		 * 结论：jdbc connector作为sink输出到mysql可以做update(也可以删除)
		 */
		env.execute();

	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Bean1 {
		public int id;
		public String gender;
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Bean2 {
		public int id;
		public String name;
	}

}
