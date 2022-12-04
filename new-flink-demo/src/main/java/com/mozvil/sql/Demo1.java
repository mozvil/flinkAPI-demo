package com.mozvil.sql;

import org.apache.flink.table.api.*;

public class Demo1 {

	/**
	 * Flinksql sql API
	 * 创建flinksql编程入口
	 * 将数据源定义成表(视图) 这里用kafka里的数据映射成一张flinksql表
	 * 执行sql语义的查询
	 * 将查询结果输出到目标表
	 * kafka输入数据：
	 * {"id":1, "name":"Thomas", "age":28, "gender":"male"}
	 * {"id":2, "name":"Ann", "age":23, "gender":"female"}
	 * {"id":3, "name":"Terry", "age":19, "gender":"male"}
	 * {"id":4, "name":"Sophia", "age":31, "gender":"female"}
	 * {"id":5, "name":"Joe", "age":27, "gender":"male"}
	 * {"id":6, "name":"Emma", "age":34, "gender":"female"}
	 * @param args
	 */
	public static void main(String[] args) {
		// 流模式
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		TableEnvironment tableEnv = TableEnvironment.create(envSettings);
		
		// 映射表
		// kafka中数据为json: {"id":1, "name":zs, "age":28, "gender":"male"}
		// 映射后表结构 table (id int, name String, age int, gender String)
		tableEnv.executeSql(
				"CREATE TABLE t_kafka (" +
				"    id int, " +
				"    name string, " +
				"    age int, " +
				"    gender string" +
				") WITH (" +
				"    'connector' = 'kafka', " + 
				"    'topic' = 'flinksql-1', " +
				"    'properties.bootstrap.servers' = 'hadoop100:9092', " +
				"    'properties.group.id' = 'testGroup', " +
				"    'format' = 'json', " +
				"    'scan.startup.mode' = 'earliest-offset', " +
				"    'json.fail-on-missing-field' = 'false', " +
				"    'json.ignore-parse-errors' = 'true'" +
				")"
		);
		
		// 执行查询并打印查询结果
		tableEnv.executeSql("SELECT gender, avg(age) as avg_age FROM t_kafka GROUP BY gender").print();

	}

}
