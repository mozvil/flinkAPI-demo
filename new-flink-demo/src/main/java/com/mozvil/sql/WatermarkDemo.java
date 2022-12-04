package com.mozvil.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WatermarkDemo {

	/**
	 * 
	 * {"guid":1,"eventId":"e02","eventTime":"2022-11-29 17:20:35.300","pageId":"p001"}
	 * {"guid":2,"eventId":"e03","eventTime":"2022-11-29 17:22:38.590","pageId":"p003"}
	 * {"guid":3,"eventId":"e02","eventTime":"2022-11-29 17:26:43.740","pageId":"p002"}
	 * {"guid":4,"eventId":"e05","eventTime":"2022-11-29 17:31:29.610","pageId":"p005"}
	 * @param args
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		/*
		tableEnv.executeSql(
				"CREATE TABLE t_event (" +
				"    guid int, " +
				"    eventId string, " +
				"    eventTime timestamp(3), " +
				"    pageId string, " +
				"    watermark for eventTime as eventTime - interval '1' second " +
				") WITH (" +
				"    'connector' = 'kafka', " + 
				"    'topic' = 'flinksql-3', " +
				"    'properties.bootstrap.servers' = 'hadoop100:9092', " +
				"    'properties.group.id' = 'groupTest', " +
				"    'scan.startup.mode' = 'earliest-offset', " +
				"    'format' = 'json', " +
				"    'json.fail-on-missing-field' = 'false', " + 
				"    'json.ignore-parse-errors' = 'true' " +
				")"	
		);
		*/
		//{"guid":4,"eventId":"e05","eventTime":"1655017434000","pageId":"p005"}
		tableEnv.executeSql(
				"CREATE TABLE t_event (" +
				"    guid int, " +
				"    eventId string, " +
				"    eventTime bigint, " +
				"    pageId string, " +
				// proctime()函数返回处理时间 自动产生和物理字段无关
				"    pt AS proctime(), " +
				// eventTime是long类型数据(1970-01-01 00:00:00起的毫秒数)需要转成timestamp类型
				"    rt as to_timestamp_ltz(eventTime, 3), " +
				// 用timesatamp类型的字段作为watermark
				"    watermark for rt as rt - interval '0.001' second " +
				") WITH (" +
				"    'connector' = 'kafka', " + 
				"    'topic' = 'flinksql-4', " +
				"    'properties.bootstrap.servers' = 'hadoop100:9092', " +
				"    'properties.group.id' = 'groupTest', " +
				"    'scan.startup.mode' = 'earliest-offset', " +
				"    'format' = 'json', " +
				"    'json.fail-on-missing-field' = 'false', " + 
				"    'json.ignore-parse-errors' = 'true' " +
				")"	
		);
		tableEnv.executeSql("desc t_event").print();
		tableEnv.executeSql("select guid, eventId, eventTime, pageId, RAND(10) as rand_num, CURRENT_WATERMARK(rt) as wm from t_event").print();

	}

}
