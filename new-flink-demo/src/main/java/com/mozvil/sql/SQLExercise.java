package com.mozvil.sql;

import static org.apache.flink.table.api.Expressions.*;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQLExercise {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		// 用SQL语句建表(连接kafka)
		tableEnv.executeSql(
				"CREATE TABLE t_person (" +
				"    id int, " + // 物理字段
				"    name string, " + // 物理字段
				"    nick string, " + // 物理字段
				"    age int, " + // 物理字段
				"    gender string, " + // 物理字段
				"    guid as id, " + // 表达式字段(逻辑字段)
				"    big_age as age + 10, " + // 表达式字段(逻辑字段)
				"    f_offset bigint metadata from 'offset', " + // 元数据字段
				"    f_ts TIMESTAMP_LTZ(3) metadata from 'timestamp' " + // 元数据字段
				//"    PRIMARY KEY(id, name) NOT ENFORCED" + // 联合主键
				") WITH (" +
				"    'connector' = 'kafka', " + 
				"    'topic' = 'flinksql-2', " +
				"    'properties.bootstrap.servers' = 'hadoop100:9092', " +
				"    'properties.group.id' = 'testGroup', " +
				"    'format' = 'json', " +
				"    'scan.startup.mode' = 'earliest-offset', " +
				"    'json.fail-on-missing-field' = 'false', " +
				"    'json.ignore-parse-errors' = 'true'" +
				")"
		);
		
		//tableEnv.executeSql("desc t_person").print();
		//tableEnv.executeSql("select * from t_person").print();
		
		// 用TableAPI方式建表(与上面SQL建表效果相同)
		tableEnv.createTable("t_person", TableDescriptor
					.forConnector("kafka")
					.schema(Schema.newBuilder()
							      .column("id", DataTypes.INT())
							      .column("name", DataTypes.STRING())
							      .column("nick", DataTypes.STRING())
							      .column("age", DataTypes.INT())
							      .column("gender", DataTypes.STRING())
							      // 表达式类型字段
							      .columnByExpression("guid", $("id"))
							      .columnByExpression("big_age", $("age").plus(10))
							      // 元数据类型字段
							      // 最后一个参数isVirtual表示：当此表被sink表时，该字段是否出现在schema中
							      .columnByMetadata("f_offset", DataTypes.BIGINT(), "offset", true)
							      .columnByMetadata("f_ts", DataTypes.TIMESTAMP_LTZ(3), "timesatamp", true)
							      //.primaryKey("id", "name")
							      .build()
					)
					.format("json")
					.option("topic", "flinksql-2")
					.option("properties.bootstrap.servers", "hadoop100:9092")
					.option("properties.group.id", "testGroup")
					.option("scan.startup.mode", "earliest-offset")
					.option("json.fail-on-missing-field", "false")
					.option("json.ignore-parse-errors", "true")
					.build());
		
		tableEnv.executeSql("desc t_person").print();
		tableEnv.executeSql("select * from t_person").print();

	}

}
