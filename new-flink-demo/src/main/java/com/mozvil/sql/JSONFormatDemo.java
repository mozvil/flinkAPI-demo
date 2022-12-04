package com.mozvil.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JSONFormatDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		/*
		tableEnv.executeSql(
				"CREATE TABLE t_json1 (" +
				"    id int, " + // 物理字段
				"    name map<string,string>, " + // 物理字段
				"    big_id as id * 10 " + // 表达式字段(逻辑字段)
				//"    file_name string metadata from 'file.name' " + // 元数据字段
				//"    PRIMARY KEY(id, name) NOT ENFORCED" + // 联合主键
				") WITH (" +
				"    'connector' = 'filesystem', " + 
				"    'path' = 'file:///D:/dev/HadoopWS/new-flink-demo/src/main/resources/json.txt', " +
				"    'format' = 'json'" +
				")"
		);
		
		tableEnv.executeSql("desc t_json1").print();
		//tableEnv.executeSql("select * from t_json1").print();
		tableEnv.executeSql("select id, name['nick'] as nick from t_json1").print();
		*/
		
		/*
		tableEnv.createTable("t_json1",
				TableDescriptor.forConnector("filesystem")
					.schema(Schema.newBuilder()
							      .column("id", DataTypes.INT())
							      .column("name", DataTypes.ROW(
							    		  DataTypes.FIELD("nick", DataTypes.STRING()),
							    		  DataTypes.FIELD("formal", DataTypes.STRING()),
							    		  DataTypes.FIELD("height", DataTypes.INT())
							      ))
							      .build()
					)
					.format("json")
					.option("path", "file:///D:/dev/HadoopWS/new-flink-demo/src/main/resources/json.txt")
					.build());
		
		tableEnv.executeSql("desc t_json1").print();
		//tableEnv.executeSql("select * from t_json1").print();
		tableEnv.executeSql("select id, name.formal as formal, name.height as height from t_json1").print();
		*/
		
		tableEnv.executeSql(
				"CREATE TABLE t_json1 (" +
				"    id int, " + // 物理字段
				"    friends array<row<name string,info map<string, string>>>" + // 复杂类型字段
				//"    PRIMARY KEY(id, name) NOT ENFORCED" + // 联合主键
				") WITH (" +
				"    'connector' = 'filesystem', " + 
				"    'path' = 'file:///D:/dev/HadoopWS/new-flink-demo/src/main/resources/json2.txt', " +
				"    'format' = 'json'" +
				")"
		);
		
		tableEnv.executeSql("desc t_json1").print();
		//tableEnv.executeSql("select * from t_json1").print();
		tableEnv.executeSql(
				"select id, friends[1].name as name1, friends[1].info['addr'] as addr1, friends[1].info['gender'] as gender1, " + 
				"friends[2].name as name2, friends[2].info['addr'] as addr2, friends[2].info['gender'] as gender2 " +
		        "from t_json1"
		).print();

	}

}
