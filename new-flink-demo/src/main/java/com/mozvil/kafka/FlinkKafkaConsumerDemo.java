package com.mozvil.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class FlinkKafkaConsumerDemo {

	public static void main(String[] args) throws Exception {
		// 获取环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("192.168.20.120:9092")
				.setTopics("test1")
				.setGroupId("test")
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
				.setValueOnlyDeserializer(new SimpleStringSchema())
				// 开启了Kafka底层消费者自动偏移量提交机制，将最新的消费位移提交到kafka的consumer_offsets中
				// 即使开启了自动提交偏移量机制，KafkaSource仍然不依赖自动偏移量提交机制(宕机重启时优先从flink自己的状态中获取偏移量)
				//.setProperty("auto.offset.commit", "true")
				.build();
		/*
		DataStream<Tuple2<String, Integer>> dataStream = env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
				.flatMap(new Splitter())
				.keyBy(value -> value.f0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.sum(1);
		dataStream.print();
		env.execute("KafkaConsumer Job");
		*/
		DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kfk-source");//  接收的是 Source 接口的实现类
		streamSource.print();
		env.execute();
	}
	
	/*
	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = -1475505302055740868L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			for(String word : value.split(" ")) {
				System.out.println(word);
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
		
	}
	*/

}
