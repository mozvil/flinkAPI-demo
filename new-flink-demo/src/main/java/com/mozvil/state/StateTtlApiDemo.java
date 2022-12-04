package com.mozvil.state;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTtlApiDemo {
	
	/**
	 * 需求场景：
	 *    有一个map算子要将用户行为数据做扩充(关联用户画像的数据后输出到下游算子)
	 *    用户画像数据存在HBase中 如果每进入一条用户行为数据都要在HBase中查询一下用户的画像信息效率很底下 可以使用Flink状态来缓存用户画像信息
	 *    当有用户行为数据进入后先到状态存储中查询一下用户画像信息 如果存在就可以直接做扩充并输出关联画像信息的用户行为数据
	 *    如果状态中没有查到该用户的画像信息则再去HBase中查询用户画像信息 查到后将此用户的画像信息存放到状态存储中做缓存(下次还有这个用户的数据就无需再去HBase中查询)
	 *    将在HBase中查到的用户画像放入状态存储后再将用户行为数据关联这个画像信息输出到下游
	 *    由于用户画像信息库非常庞大 如果将所有的用户画像信息全部存放到状态存储中会导致内存耗尽 查询速度也会非常慢
	 *    所以考虑只存放那些热点数据(最近一段时间有查询过的画像信息) TTL设置的数据存活时间将决定状态存储中的数据何时被清除
	 *    除了设置一个数据存活时间 Flink还有一个机制：
	 *       加入一条数据即将到了其存活时间(马上就要被清除) 这时它又被查询了一次那么可以将它的存活时间重新开始计算(这条数据再一次变成热点数据)
	 *       比如状态中设置了TTL的时间为10分钟，某条数据前9分钟未被查询在到10分钟前的几秒钟内又被查询了一次，那么它的存活时间将重新变成10分钟
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		// 开启状态托管Checkpointing
		// 参数1：快照获取时间间隔  参数2(可不传 默认就是EXACTLY_ONCE)：Checkpointing模式
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		// 开启Task故障重启 Flink有多种Task级别的重启机制默认为RestartStrategies.noRestart()(不会重启)
		// 使用RestartStrategies.fixedDelayRestart(固定延迟重启) 参数1：重启次数限制  参数2：每两次重启间隔时间
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
		
		// 状态后端设置(默认为HashMapStateBackend)
		//HashMapStateBackend hasMapStateBackend = new HashMapStateBackend();
		//env.setStateBackend(hasMapStateBackend);
		EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
		env.setStateBackend(embeddedRocksDBStateBackend);
		
		
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		
		sourceStream
				.keyBy(s -> "0")
				.map(new RichMapFunction<String, String>() {
		
					private static final long serialVersionUID = -3079059601038654002L;
			
					private ValueState<String> valueState;
					private ListState<String> listState;
			
					@Override
					public void open(Configuration parameters) throws Exception {
						RuntimeContext runtimeContext = getRuntimeContext();
						// 设置TTL(Time To Live)
						StateTtlConfig ttlConfig = StateTtlConfig
								// 配置状态存储中数据的存活时长为5秒
								.newBuilder(Time.milliseconds(5000))
								// 同上配置数据存活时长
								//.setTtl(1000 * 60)
								// 当插入和更新数据时此条数据的ttl计时将重置
								//.updateTtlOnCreateAndWrite()
								// 当发生这条数据的读写操作时此条数据的ttl计时将重置
								// updateTtlOnReadAndWrite()与updateTtlOnCreateAndWrite()选择一个 后面的覆盖前面的
								//.updateTtlOnReadAndWrite()
								// 状态存储中的数据存活时间到达后将不再返回此条数据(默认)
								.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
								// 状态存储中的数据存活时间到达后如果此条数据还未被清除则可以返回此条数据
								//.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
								// ttl计时器的时间语义(默认为处理时间)
								.setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
								.useProcessingTime()
								// 增量清理策略：每当一条数据被访问 这条数据会被检查是否已过期 如果已过期就会被清理掉
								// 参数#1 1000表示清理时一次迭代5个Key(状态存储是以Key/Value的形式保存在内存中的) 这个参数值越大清理检查的数据量就越大
								// 参数#2 为true则参数一失效 此参数意义为是否在每次访问状态存储数据时候遍历检查所有数据是否已经过期
								//       不建议开启参数#2 由于访问状态数据会驱动数据检查 这样会使访问状态存储数据的速度变慢
								//.cleanupIncrementally(1000, false)
								// 全量快照清理策略：在状态做快照的时候会判断数据是否过期(快照中存放的是未过期的数据)
								// 快照清理不影响本地内存中的状态数据(内存不会清理数据)
								//.cleanupFullSnapshot()
								// Flink底层(后端)存储状态的实现有两种：
								//     #1 HashMapStateBackend (默认)数据以java对象的形式存放在heap中 如果内存溢出会保存到磁盘上
								//                            优点：内存中获取数据较快 缺点：一旦内存溢出后从磁盘上读取数据速度就很慢
								//     #2 EmbeddedRocksDBStateBackend 数据以序列化后的字节形式存放在RocksDb数据库中 数据存放也(优先)使用内存 如果内存溢出会保存到磁盘上
								//                            RocksDb是一种以键值对形式存储数据的内嵌型数据库(类似于Derby Derby是关系型的内嵌数据库)
								//                            优缺点和HashMapStateBackend正好相反
								// 两种状态后端生成CheckPointing快照文件的格式完全一致
								// 结论：如果需要存储超大量的数据在状态中就需要使用EmbeddedRocksDBStateBackend 反之使用HashMapStateBackend(即两者中选择视状态数据量大小)
								// cleanupInRocksdbCompactFilter()只对RocksDbStateBackend状态后端实现有效
								// 利用RocksDb做Compact(数据合并清理)加入清理过期状态数据的逻辑
								//.cleanupInRocksdbCompactFilter(1000)
								.build();
						ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("vstate", String.class);
						// 开启TTL管理
						valueStateDescriptor.enableTimeToLive(ttlConfig);
						valueState = runtimeContext.getState(valueStateDescriptor);
						
						// 注：如果使用listState状态存储 则list中的每条数据各自有一个独立TTL
						//    如果使用mapState状态存储 则map中的每对key/value各自有一个独立TTL
						ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<String>("list", String.class);
						listStateDescriptor.enableTimeToLive(ttlConfig);
						listState = runtimeContext.getListState(listStateDescriptor);
					}
		
					@Override
					public String map(String value) throws Exception {
						listState.add(value);
						StringBuilder sb = new StringBuilder();
						for(String s : listState.get()) {
							sb.append(s);
						}
						return sb.toString();
					}
			
				})
				.print();
		
		env.execute();
	}

}
