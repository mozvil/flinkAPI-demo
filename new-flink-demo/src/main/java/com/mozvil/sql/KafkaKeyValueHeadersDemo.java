package com.mozvil.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaKeyValueHeadersDemo {
	
	/**
	 * 使用offsetexplorer生产kafka数据
	 * kafka数据：
	 * KEY: {"k1":100, "k2":200}
	 * VALUE: {"guid":1,"eventId":"e02","eventTime":"2022-11-29 17:20:35.300","pageId":"p001"}
	 * HEADERS:
	 *     header1:value1
	 *     header2:value2
	 * @param args
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		tableEnv.executeSql(
				"CREATE TABLE t_kafka_detail (" +
				"    guid int, " +
				"    eventId string, " +
				"    eventTime bigint, " + 
				"    pageId string, " +
				"    k1 int, " +
				"    k2 int, " +
				"    f_offset bigint metadata from 'offset', " + // 元数据字段
				"    f_ts timestamp(3) metadata from 'timestamp', " + // 元数据字段
				"    headers map<string,bytes> metadata, " + // 元数据字段(headers)
				"    rt as to_timestamp_ltz(eventTime,3), " +
				"    watermark for rt as rt - interval '0.001' second " +
				//"    PRIMARY KEY(id, name) NOT ENFORCED" + // 联合主键
				") WITH (" +
				"    'connector' = 'kafka', " + 
				"    'topic' = 'flinksql-5', " +
				"    'properties.bootstrap.servers' = 'hadoop100:9092', " +
				"    'properties.group.id' = 'testGroup', " +
				"    'scan.startup.mode' = 'earliest-offset', " +
				"    'key.format' = 'json', " +
				"    'key.json.ignore-parse-errors' = 'true', " +
				"    'key.fields' = 'k1;k2', " +
				//"    'key.fields-prefix' = '', " +
				"    'value.format' = 'json', " +
				"    'value.json.fail-on-missing-field' = 'false', " +
				"    'value.fields-include' = 'EXCEPT_KEY'" +
				")"
		);
		
		tableEnv.executeSql("select * from t_kafka_detail").print();
		// 查headers中value值
		tableEnv.executeSql("select guid, eventId, cast(headers['header1'] as string) as h1, cast(headers['header2'] as string) as h2 from t_kafka_detail").print();
		

	}

}
