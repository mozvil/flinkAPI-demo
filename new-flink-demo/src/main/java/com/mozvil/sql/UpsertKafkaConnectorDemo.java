package com.mozvil.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class UpsertKafkaConnectorDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		// 数据：1, male
		DataStreamSource<String> stream = env.socketTextStream("hadoop100", 9998);
		SingleOutputStreamOperator<CountPojo> beanStream = stream.map(s -> {
			String[] arr = s.split(",");
			return new CountPojo(Integer.parseInt(arr[0]), arr[1]);
		});
		
		tableEnv.createTemporaryView("t_bean", beanStream);
		//tableEnv.executeSql("select gender, count(1) as cnt from t_bean group by gender").print();
		// 建sink表(流转表)
		tableEnv.executeSql(
				"create table t_upsert_kafka (" +
				"    gender string primary key not enforced, " +
				"    cnt bigint" + 
				") with (" + 
				"    'connector' = 'upsert-kafka', " +
				"    'topic' = 'flinksql-5', " +
				"    'properties.bootstrap.servers' = 'hadoop100:9092', " +
				"    'key.format' = 'csv', " +
				"    'value.format' = 'csv'" +
				")"
		);
		tableEnv.executeSql("insert into t_upsert_kafka select gender, count(1) as cnt from t_bean group by gender");
		tableEnv.executeSql("select * from t_upsert_kafka").print();
		
		env.execute();

	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CountPojo {
		
		public int id;
		public String gender;
		
	}

}
