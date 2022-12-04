package com.mozvil.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import com.alibaba.fastjson2.JSON;

/**
 * 创建Table对象的各种方法
 */
public class TableObjectCreateDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		env.setParallelism(1);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		// #1 用一个已存在的表名创建Table对象
		Table table_a = tableEnv.from("table_a");
		table_a.execute().print();
		
		// #2 用一个TableDescriptor创建一个Table对象
		Table table_b = tableEnv.from(
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
		table_b.execute().print();
		
		// #3 用数据流创建Table对象
		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("hadoop100:9092")
				.setTopics("flinksql-1")
				.setGroupId("testGroup")
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
		DataStreamSource<String> kfkSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk");		
		
		// 先把数据流中的数据转成Person bean
		SingleOutputStreamOperator<Person> beanStream = kfkSource.map(json -> JSON.parseObject(json, Person.class));
		tableEnv.fromDataStream(beanStream, 
				Schema.newBuilder()
			      .column("id", DataTypes.INT())
			      .column("name", DataTypes.STRING())
			      .column("age", DataTypes.INT())
			      .column("gender", DataTypes.STRING())
			      .build()).execute().print();
		
		// #4 使用测试数据创建Table对象
		// 单字段表
		Table table_d1 = tableEnv.fromValues(1, 2, 3, 4, 5);
		table_d1.printSchema();
		table_d1.execute().print();
		
		// 多字段表(指定字段名)
		Table table_d2 = tableEnv.fromValues(
				DataTypes.ROW(
						DataTypes.FIELD("id", DataTypes.INT()),
						DataTypes.FIELD("name", DataTypes.STRING()),
						DataTypes.FIELD("age", DataTypes.DOUBLE()),
						DataTypes.FIELD("gender", DataTypes.STRING())
				),
				Row.of(1, "aa", 1, "M"),
				Row.of(2, "bb", 2, "F"),
				Row.of(3, "cc", 3, "M"),
				Row.of(4, "dd", 4, "F")
		);
		table_d2.printSchema();
		table_d2.execute().print();
		
		
		env.execute();
	}

}
