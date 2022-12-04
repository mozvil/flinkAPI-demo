package com.mozvil.tolerance;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import com.mozvil.sink.NewJdbcSink;
import com.mysql.cj.jdbc.MysqlXADataSource;

public class SecondTest {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
		
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		
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
		
		SingleOutputStreamOperator<String> stream2 = stream1
			.keyBy(s -> "0")
			.map(new RichMapFunction<String, String>() {
				
				private static final long serialVersionUID = -3079059601038654002L;
		
				private ValueState<String> valueState;
		
				@Override
				public void open(Configuration parameters) throws Exception {
					RuntimeContext runtimeContext = getRuntimeContext();
					ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("vstate", String.class);
					valueState = runtimeContext.getState(valueStateDescriptor);
				}
	
				@Override
				public String map(String element) throws Exception {
					String preStr = valueState.value();
					if(preStr == null) {
						preStr = "";
					}
					System.out.println("pre: " + preStr);
					valueState.update(element);
					
					// 这里埋一个异常：当接收到x时会有1/3的概率发生异常
					if(element.equals("x") && RandomUtils.nextInt(1, 4) == 0) {
						throw new Exception("Supposed Exception Invoked!");
					}
					return preStr + "-" + element.toUpperCase();
				}
		
			});
		
		stream2.print();
		
		// Sink构建
		/*
		SinkFunction<String> exactlyOnceJdbcSink = JdbcSink.exactlyOnceSink(
				"insert into t_eos values (?) on duplicate key update str = ?", 
				new JdbcStatementBuilder<String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void accept(PreparedStatement stmt, String s) throws SQLException {
						System.out.println("value: " + s);
						stmt.setString(1, s);
						stmt.setString(1, s);
					}
					
				}, 
				JdbcExecutionOptions.builder().withMaxRetries(0).build(), 
				JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(), 
				() -> {
					MysqlXADataSource xaDataSource = new MysqlXADataSource();
					xaDataSource.setDatabaseName("flink_test");
					xaDataSource.setUrl("jdbc:mysql://192.168.20.100:3306");
					xaDataSource.setUser("root");
					xaDataSource.setPassword("12345678");
					return xaDataSource;
				}
		);
				
		stream2.addSink(exactlyOnceJdbcSink).name("sinkToMysql");
		*/
		stream2.addSink(new NewJdbcSink());
		
		
		env.execute();
	}

}
