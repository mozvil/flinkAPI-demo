package com.mozvil.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorStateDemo {
	
	/**
	 * 需求：
	 *     记录每次输入的字符串并拼接到下一条记录之前
	 *     如 输入a  输出a
	 *        再输入b 输出ab
	 *        再输入c 输出abc
	 *        再输入d 输出abcd
	 * 利用Flink的算子状态存储管理器实现
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
		
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		sourceStream.map(new StateMapFunction()).print();
		// 如果把map算子的并行度设置为2 那么这2个算子将各自维护一个状态存储管理器(相互之间的状态数据是独立的)
		//sourceStream.map(new StateMapFunction()).setParallelism(2).print().setParallelism(2);
		
		env.execute();

	}

}
