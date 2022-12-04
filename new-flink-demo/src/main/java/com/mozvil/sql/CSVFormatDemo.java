package com.mozvil.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CSVFormatDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		tableEnv.executeSql(
				"CREATE TABLE t_csv1 (" +
				"    id int, " +
				"    name string, " +
				"    age int " +
				") WITH (" +
				"    'connector' = 'filesystem', " + 
				"    'path' = 'file:///D:/dev/HadoopWS/new-flink-demo/src/main/resources/csv.txt', " +
				"    'format' = 'csv', " +
				"    'csv.quote-character' = '\"', " + //字段数据的包围字符
				"    'csv.disable-quote-character' = 'false', " + //是否禁用字段数据的包围字符
				"    'csv.ignore-parse-errors' = 'true', " +
				"    'csv.null-literal' = 'AA', " + //把AA看作是<NULL>
				"    'csv.allow-comments' = 'true' " +
				")"
		);
		
		tableEnv.executeSql("desc t_csv1").print();
		tableEnv.executeSql("select * from t_csv1").print();

	}

}
