package com.mozvil.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlTableCreateDemo {

	/**
	 * 创建带名字的表(可以使用sql语句查询的表)的各种方法
	 * @param args
	 */
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		// #1 读取src/main/resources下的data.txt文件中的数据创建一张带名字的表
		/*
		tableEnv.createTable("table_a",
				TableDescriptor.forConnector("filesystem")
					.schema(Schema.newBuilder()
							      .column("id", DataTypes.INT())
							      .column("name", DataTypes.STRING())
							      .column("age", DataTypes.INT())
							      .column("gender", DataTypes.STRING())
							      .build()
					)
					.format("csv")
					.option("path", "file:///D:/dev/HadoopWS/new-flink-demo/src/main/resources/data.txt")
					.option("csv.allow-comments", "true")
					.option("csv.ignore-parse-errors", "true")
					.build());
		// 可以直接将table_a表转成一个Table对象
		//Table table_a = tableEnv.from("table_a");
		
		// 执行sql查询并打印
		tableEnv.executeSql("select * from table_a").print();
		tableEnv.executeSql("select gender, avg(age) as avg_age FROM table_a GROUP BY gender").print();
		*/
		
		// #2 通过dataStream(Socket)创建一个带名字的视图
		DataStreamSource<String> stream = env.socketTextStream("hadoop100", 9998);
		SingleOutputStreamOperator<Person> beanStream = stream.map(s -> {
				String[] split = s.split(",");
				return new Person(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]), split[3]);
		});
		tableEnv.createTemporaryView("t_person", beanStream);
		tableEnv.executeSql("select gender, max(age) as max_age from t_person group by gender").print();
		
		// 通过一个已存在的Table对象创建一个带名字的视图
		Table table_a = tableEnv.from("table_a");
		tableEnv.createTemporaryView("table_x", table_a);
		tableEnv.executeSql("select * from table_x").print();

	}

}
