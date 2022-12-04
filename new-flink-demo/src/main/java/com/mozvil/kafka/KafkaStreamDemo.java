package com.mozvil.kafka;

import java.util.HashMap;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import com.mozvil.demo.Event;

public class KafkaStreamDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 1 kafka读取数据
		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("hadoop100:9092,hadoop101:9092")
				.setTopics("first")
				.setGroupId("test")
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setProperty("auto.offset.commit", "true")
				.build();
		DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk-source");
		
		// 2 用flink进行转换处理
		SingleOutputStreamOperator<String> result = streamSource.map(new MapFunction<String, String>() {

			private static final long serialVersionUID = -5833654714885044828L;

			@Override
			public String map(String value) throws Exception {
				String[] fields = value.split(",");
				return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()), new HashMap<String, String>()).toString();
			}
		});
		
		// 3 结果数据写入kafka
		KafkaSink<String> kafkaProducer = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop100:9092,hadoop101:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("second")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()).build();
		
		// 添加数据源
		result.sinkTo(kafkaProducer);
		// 执行代码
		env.execute();
	}

}
