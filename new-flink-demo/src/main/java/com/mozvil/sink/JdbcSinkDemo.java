package com.mozvil.sink;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.XADataSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import com.alibaba.fastjson.JSON;
import com.mozvil.demo.Event;
import com.mozvil.demo.TestRichCusomSource;
import com.mysql.cj.jdbc.MysqlXADataSource;

public class JdbcSinkDemo {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		// http://localhost:8081/
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setParallelism(2);
		
		DataStreamSource<Event> dataStreamSource = env.addSource(new TestRichCusomSource());
		
		// JDBC sink有两种构造方式
		
		// 1. JdbcSink.sink() 这种sink方式底层没有开启mysql的事务所以无法保证"端到端"的精确一次性
		/*
		SinkFunction<Event> jdbcSink = JdbcSink.sink(
				// 参数#1 准备执行的SQL语句
				"INSERT INTO t_event (user_name, event_id, event_time, event_info) values (?, ?, ?, ?)",
				// 参数#2 设置SQL参数
				new JdbcStatementBuilder<Event>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void accept(PreparedStatement stmt, Event event) throws SQLException {
						stmt.setString(1, event.getUser());
						stmt.setString(2, event.getEventId());
						stmt.setLong(3, event.getTimestamp());
						stmt.setString(4, JSON.toJSONString(event.getEventInfo()));
					}
					
				}, 
				// 参数#3 设置批量执行(数量) 失败重试次数 批次执行间隔(毫秒)
				JdbcExecutionOptions.builder().withBatchSize(5).withMaxRetries(2).withBatchIntervalMs(1000).build(), 
				// 参数#4 JDBC连接信息
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
					.withDriverName("com.mysql.cj.jdbc.Driver")
					.withUrl("jdbc:mysql://192.168.20.130:3306/flink_test")
					.withUsername("root")
					.withPassword("QAZwsx12345678")
					.withConnectionCheckTimeoutSeconds(5)
					.build());
		dataStreamSource.addSink(jdbcSink);
		*/
		
		// 2. JdbcSink.eactlyOnceSink 这种sink方式可以保证"端到端"的精确一次性(底层利用JDBC目标数据库的事务机制)
		SinkFunction<Event> jdbcEoSink = JdbcSink.exactlyOnceSink(
				// 参数#1 准备执行的SQL语句
				"INSERT INTO t_event (user_name, event_id, event_time, event_info) values (?, ?, ?, ?)", 
				// 参数#2 设置SQL参数
				new JdbcStatementBuilder<Event>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void accept(PreparedStatement stmt, Event event) throws SQLException {
						stmt.setString(1, event.getUser());
						stmt.setString(2, event.getEventId());
						stmt.setLong(3, event.getTimestamp());
						stmt.setString(4, JSON.toJSONString(event.getEventInfo()));
					}
					
				}, 
				// 参数#3 设置批量执行(数量) 失败重试次数 批次执行间隔(毫秒)
				JdbcExecutionOptions.builder().withMaxRetries(0).build(), 
				// 参数#4 Mysql不支持在同一个连接上开启多个并行事务
				JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),
				// 参数#5 JDBC连接信息 
				() -> {
					// 分布式事务DataSource
					// Mysql数据库中需要设置用户的XA_RECOVER_ADMIN权限: 
					//     use mysql;
					//     GRANT XA_RECOVER_ADMIN ON *.* TO 'root'@'%';
					MysqlXADataSource xaDataSource = new MysqlXADataSource();
					//xaDataSource.setDatabaseName("flink_test");
					xaDataSource.setUrl("jdbc:mysql://192.168.20.130:3306/flink_test");
					xaDataSource.setUser("root");
					xaDataSource.setPassword("QAZwsx12345678");
					return xaDataSource;
				});
		
		dataStreamSource.addSink(jdbcEoSink).name("sinkToMysql");
		
		env.execute();
	}

}
