package com.mozvil.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;
import com.mozvil.demo.Event;
import com.mozvil.demo.TestRichCusomSource;

public class FlinkKafkaProducerDemo {

	public static void main(String[] args) throws Exception {
		// 获取环境
		Configuration configuration = new Configuration();
		// http://localhost:8081/
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setParallelism(1);
		
		// 准备数据源
		/*
		List<String> wordList = new ArrayList<String>();
		wordList.add("hello");
		wordList.add("mozvil");
		wordList.add("李冬亮是个大傻X");
		DataStreamSource<String> stream = env.fromCollection(wordList);
		*/
		DataStreamSource<Event> dataStreamSource = env.addSource(new TestRichCusomSource()).setParallelism(2);
		
		// 创建kafka生产者
		// Properties properties = new Properties();
		// properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop100:9092,hadoop101:9092");
		//FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("first", new SimpleStringSchema(), properties);
		
		KafkaSink<String> kafkaProducer = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.20.120:9092")
                .setRecordSerializer(
                		KafkaRecordSerializationSchema.builder()
                			.setTopic("first")
                			.setValueSerializationSchema(new SimpleStringSchema())
                			.build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("mozvil-")
                .build();
		
		// 添加数据源
		dataStreamSource.map(JSON::toJSONString).disableChaining().sinkTo(kafkaProducer);
		
		// 执行代码
		env.execute();
	}

}
