package com.mozvil.sql;

import static org.apache.flink.table.api.Expressions.*;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableAPIDemo {
	
	/**
	 * Flinksql table API
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
		// 纯表环境
		//EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		//TableEnvironment tableEnv = TableEnvironment.create(envSettings);
		
		// 混合环境创建(流+表)
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		// 建表
		Table table = tableEnv.from(
				TableDescriptor
					.forConnector("kafka")
					.schema(Schema.newBuilder()
							      .column("id", DataTypes.INT())
							      .column("name", DataTypes.STRING())
							      .column("age", DataTypes.INT())
							      .column("gender", DataTypes.STRING())
							      .build()
					)
					.format("json")
					.option("topic", "flinksql-1")
					.option("properties.bootstrap.servers", "hadoop100:9092")
					.option("properties.group.id", "testGroup")
					.option("scan.startup.mode", "earliest-offset")
					.option("json.fail-on-missing-field", "false")
					.option("json.ignore-parse-errors", "true")
					.build());
		
		// 查询
		// 可以使用from()方法传入一个表名获取table对象
		//Table queryTable = tableEnv.from("t_kafka");
		Table queryTable = table.groupBy($("gender")).select($("gender"), $("age").avg().as("avg_age"));
		
		// 将一个已创建的table对象注册成sql中的视图
		//tableEnv.createTemporaryView("t_kafka", table);
		// 这样可以直接使用sql在注册的视图中查询
		// tableEnv.executeSql("select * from t_kafka").print();
		
		// 打印
		queryTable.execute().print();
	}

}
