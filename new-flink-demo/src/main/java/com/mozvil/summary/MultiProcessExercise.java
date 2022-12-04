package com.mozvil.summary;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSON;
import com.mozvil.demo.Event;
import com.mysql.cj.jdbc.MysqlXADataSource;

public class MultiProcessExercise {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setParallelism(8);
		
		/**
		 * 创建两个流
		 * #1  id,event,count
         *     1,event01,3
         *     1,event02,2
         *     2,event02,4
         *
         * #2  id,gender,city
         *     1,male,shanghai
         *     2,femal,beijing
		 */
		DataStreamSource<String> stream1 = env.socketTextStream("hadoop100", 9998);
		DataStreamSource<String> stream2 = env.socketTextStream("hadoop100", 9999);
		
		/**
		 * 1. 将流#1的数据展开，如：
         *   1,event01,3
         *   展开成3条数据：
         *     1,event01,随机数1
         *     1,event01,随机数2
         *     1,event01,随机数3
		 */
		SingleOutputStreamOperator<EventInfo> userEventStream = stream1.process(new ProcessFunction<String, EventInfo>() {

			private static final long serialVersionUID = -3588304434753892472L;
			
			private Random r = new Random();

			@Override
			public void processElement(String value, 
					ProcessFunction<String, EventInfo>.Context context,
					Collector<EventInfo> out) throws Exception {
				String[] arr = value.split(",");
				int count = Integer.valueOf(arr[2]);
				for(int i=0; i<count; i++) {
					out.collect(new EventInfo(Integer.valueOf(arr[0]), arr[1], r.nextInt(1000)));
				}
			}

		});
		
		// 通过SingleOutputStreamOperator的slotSharingGroup可以设定一个算子的slot槽位共享组(组名通过参数传入)
		// 设置槽位共享组的意义：
		//      如果有两个计算任务繁重的算子(原来符合绑定在同一个Task中的条件可以绑定)，但考虑到在同一个实例(同一个线程中 即在同一个slot)中运行性能问题
		//      通过指定不同槽位共享组名，强制将这两个算子拆分开(不允许绑定在同一个Task实例中)，这样Flink就不会再将他们绑定合并到同一个Task实例(同一个线程 即在同一个slot)中
		// 如果在上游算子中设定了槽位共享组名，下游的算子默认会继承这个共享组名，这样并未达到拆分算子到不同slot中的效果
		// 需要在下游算子中设定不同的槽位共享组名，Flink才会将这些不同槽位共享组名的算子拆分到不同slot中去
		// 再下游的算子依然会继承新的槽位共享组名，所以如果需要将后续的算子也与当前算子断开，那么后续下游的算子也要设置不同的槽位共享组名
		// e.g. op1算子(共享组g1) op2算子(共享组g2) op3(共享组g1)
		//    上述情况下op1与op3可以共享一个slot 而op2既不会与op1共享一个slot也不会与op3共享一个slot
		// 另外SingleOutputStreamOperator的disableChaining()可以强制断开算子链，这样它和上下游的算子就都不会合并在一个Task中
		// SingleOutputStreamOperator的startNewChain()方法开启新链(前面断开)
		SingleOutputStreamOperator<UserInfo> userInfoStream = stream2.map(s -> {
			String[] arr = s.split(",");
			return new UserInfo(Integer.valueOf(arr[0]), arr[1], arr[2]);
		}).returns(UserInfo.class).slotSharingGroup("s1");
		
		// 创建一个状态(由Flink管理的状态 有持久化)描述
		MapStateDescriptor<Integer, UserInfo> userInfoStateDesc = new MapStateDescriptor<Integer, UserInfo>(
				"userInfoState",
				TypeInformation.of(Integer.class),
				TypeInformation.of(UserInfo.class)
		);
		
		BroadcastStream<UserInfo> broadCastStream = userInfoStream.broadcast(userInfoStateDesc);
		
		// 2. 将流#1展开后的数据通过id字段关联流#2的数据(gender,city)
		// 3. 将无法关到广播流的数据做测流输出，其他数据保留在主流中
		BroadcastConnectedStream<EventInfo, UserInfo> connectedStream = userEventStream.connect(broadCastStream);
		
		SingleOutputStreamOperator<UserEvent> linkedStream = connectedStream.process(
				new BroadcastProcessFunction<EventInfo, UserInfo, UserEvent>() {

			private static final long serialVersionUID = 5698957611809003179L;
			
			@Override
			public void processElement(EventInfo value,
					BroadcastProcessFunction<EventInfo, UserInfo, UserEvent>.ReadOnlyContext context,
					Collector<UserEvent> out) throws Exception {
				// 通过上下文获取一个只读的广播数据状态
				ReadOnlyBroadcastState<Integer, UserInfo> broadcastState = context.getBroadcastState(userInfoStateDesc);
				
				// 把数据加宽(注意需要判断状态是否为空或状态中的数据二元组是否获取不到也为空)
				UserInfo userInfo;
				if(broadcastState != null && (userInfo = broadcastState.get(value.getId())) != null) {
					out.collect(new UserEvent(value.getId(), userInfo.getGender(), userInfo.getCity(), value.getEventName(), value.getEventCount()));
				} else {
					context.output(new OutputTag<EventInfo>("noUserInfo", TypeInformation.of(EventInfo.class)), value);
				}
			}
			
			@Override
			public void processBroadcastElement(UserInfo value,
					BroadcastProcessFunction<EventInfo, UserInfo, UserEvent>.Context context,
					Collector<UserEvent> out) throws Exception {
				// 获取被Flink管理的状态
				BroadcastState<Integer, UserInfo> broadcastState = context.getBroadcastState(userInfoStateDesc);
				// 将广播流里的数据放入状态中
				broadcastState.put(value.getId(), value);
			}
			
		}).slotSharingGroup("g2");
		
		// 4.将主流数据按gender字段分组，取最大随机数所在的那一条数据作为结果输出
		SingleOutputStreamOperator<UserEvent> keyedStream = linkedStream.keyBy(data -> data.getGender()).maxBy("eventCount");
		
		// 5. 将主流处理结果写入mysql，并实现幂等更新
		SinkFunction<UserEvent> jdbcEoSink = JdbcSink.exactlyOnceSink(
				// 参数#1 准备执行的SQL语句
				"UPDATE t_user_event SET user_id=?, event_name=?, event_count=?, city=? where gender=?", 
				// 参数#2 设置SQL参数
				new JdbcStatementBuilder<UserEvent>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void accept(PreparedStatement stmt, UserEvent event) throws SQLException {
						System.out.println("UserEvent Content: " + event.toString());
						stmt.setInt(1, event.getId());
						stmt.setString(2, event.getEventName());
						stmt.setInt(3, event.getEventCount());
						stmt.setString(4, event.getCity());
						stmt.setString(5, event.getGender());
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
					xaDataSource.setUrl("jdbc:mysql://192.168.20.100:3306/flink_test");
					xaDataSource.setUser("root");
					xaDataSource.setPassword("12345678");
					return xaDataSource;
				});
		
		keyedStream.addSink(jdbcEoSink).name("sinkToMysql");
		
		// 5. 将侧流输出的数据写入文件系统(parquet格式)
		// 获取%7-result测流
		DataStream<EventInfo> sideStream = linkedStream.getSideOutput(
				new OutputTag<EventInfo>("noUserInfo", TypeInformation.of(EventInfo.class))
		);
		ParquetWriterFactory<EventInfo> writerFactory = AvroParquetWriters.forReflectRecord(EventInfo.class);
		FileSink<EventInfo> parquetSink2 = FileSink
				.forBulkFormat(new Path("D:/flinkSink/bulksink/"), writerFactory)
				// bulk模式下的文件滚动策略只有一种(当checkpoint发生时文件开始滚动)
				.withRollingPolicy(OnCheckpointRollingPolicy.build())
				.withBucketAssigner(new DateTimeBucketAssigner<EventInfo>())
				.withBucketCheckInterval(5)
				.withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("mozvil").withPartSuffix(".parquet").build())
				.build();
		sideStream.sinkTo(parquetSink2);
		
		env.execute();

	}

}
