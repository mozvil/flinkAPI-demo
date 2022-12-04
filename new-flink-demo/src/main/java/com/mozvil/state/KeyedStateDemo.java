package com.mozvil.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStateDemo {

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
		
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		// 使用keyed状态存储器
		// keyed状态存储和算子的状态存储区别：
		//      算子状态存储为每个key单独划分一个空间来保存状态
		//      keyed状态存储则为每个key单独划分一个空间来保存状态(每个key都独立拥有自己的状态存储空间 不同key之间不共享状态)
		sourceStream
			// 让所有的数据key都相同则只有一个状态存储空间
			.keyBy(s -> s)
			.map(new RichMapFunction<String, String>() {

				private static final long serialVersionUID = -3079059601038654002L;
				
				private ListState<String> listState;
				
				@Override
				public void open(Configuration parameters) throws Exception {
					RuntimeContext runtimeContext = getRuntimeContext();
					
					// 获取一个单值结构的状态存储器
					// runtimeContext.getState()
					
					// 获取一个Map结构的状态存储器
					//runtimeContext.getMapState()
					
					// 获取一个List结构的状态存储器
					listState = runtimeContext.getListState(new ListStateDescriptor<String>("list", Types.STRING));
				}

				@Override
				public String map(String value) throws Exception {
					// 拼接所有数据
					listState.add(value);
					StringBuilder sb = new StringBuilder();
					for(String s : listState.get()) {
						sb.append(s);
					}
					return sb.toString();
				}
				
			}).print();
		
		env.execute();
	}

}
