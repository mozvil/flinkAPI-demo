package com.mozvil.tolerance;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.XADataSource;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import com.mozvil.sink.NewJdbcSink;
import com.mysql.cj.jdbc.MysqlXADataSource;

public class ToleranceSideToSideTest {
	/**
	 * 测试Flink容错(包含端到端一致性EOS)能力
	 * 流程：
	 *    1. 从kafka读数据(source中有operator-state状态数据)
	 *    2. 用map算子处理数据(使用keyed-state状态) 逻辑：把输入的字符串变成大写并拼接此前的字符串后输出
	 *    3. 用exactly-once的mysql-sink算子输出数据(附带逐渐的幂等特性)
	 * 测试使用kafka topic创建命令: kafka-topics.sh --bootstrap-server hadoop100:9092 --create --topics eos --partitions 1 --replication-factor 1 
	 *                            kafka-topics.sh --bootstrap-server hadoop100:9092 --create --topic first  --partitions 1 --replication-factor 1
	 * 输入数据： a  b  c  d
	 * 测试用mysql表：t_eos (str varchar(128) primary key) 一个字段
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		// 指定从某个savepoint恢复状态
		//configuration.setString("execution.savepoint.path", "file:///D:/checkpoint/7ecbd4f9106957c42109bcde/chk-544");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		
		/**
		 * 容错相关参数设置
		 */
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(1000)));
		//env.setStateBackend(new HashMapStateBackend());
		
		// source构造
		KafkaSource<String> sourceOperator = KafkaSource.<String>builder()
				.setBootstrapServers("hadoop100:9092")
				.setTopics("eos")
				.setGroupId("eos01")
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
				.setProperty("commit.offsets.on.checkpoint", "false")
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
				.build();
		DataStreamSource<String> stream1 = env.fromSource(sourceOperator, WatermarkStrategy.noWatermarks(), "kfksource");
		
		// 算子构建
		SingleOutputStreamOperator<String> stream2 = stream1.keyBy(s -> "group1")
		       .map(new RichMapFunction<String, String>() {

					private static final long serialVersionUID = 1643469877755418642L;
					
					ValueState<String> valueState;
					
					@Override
					public void open(Configuration parameters) throws Exception {
						valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("preStr", Types.STRING));
					}
	
					@Override
					public String map(String element) throws Exception {
						String preStr = valueState.value();
						if(preStr == null) {
							preStr = "";
						}
						valueState.update(element);
						
						// 这里埋一个异常：当接收到x时会有1/3的概率发生异常
						if(element.equals("x") && RandomUtils.nextInt(1, 4) % 3 == 0) {
							throw new Exception("Supposed Exception Invoked!");
						}
						System.out.println("Out: " + preStr + "-" + element.toUpperCase());
						return preStr + "-" + element.toUpperCase();
					}
		    	   
		       }
		);
		
		stream2.addSink(new NewJdbcSink()).name("sinkToMysql");
		
		env.execute();
	}

}
