package com.mozvil.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import com.mozvil.demo.Event;
import com.mozvil.demo.TestRichCusomSource;

public class RedisSinkDemo {
	
	/**
	 * 测试：
	 *     .\redis-cli.exe
	 *     查看所有的key: key *
	 *     查看eventlogs这个key下的list数据(从0到5条): lrange eventlogs 0 5
	 *     
	 * Sink算子的容错机制EOS(端到端的精确一次性)有两种：
	 *     #1 幂等写入方式(需要输出目标为K/V存储类型的系统 如HBase Redis) 
	 *        幂等写入的方式可能会出现过程中的不一致 这种不一致主要出现在“输出结果非确定”的计算场景中 如增加insert_time字段或随机数
	 *     #2 使用事务写入(Kafka支持伪事务)
	 *     
	 * Sink算子两阶段事务写入(EOS)
     *     Sink算子的两阶段事务提交利用了checkpointing两阶段提交协议和目标存储系统的事务支持机制(目标系统必须支持事务)
     * 核心过程如下：
     *    0. 预提交阶段(第一阶段)存储本次对外事务号以及事务状态pending)
     *    1. Sink算子在一批数据处理过程中先通过预提交(开启)事务开始对外输出数据
     *    2. 等待这批数据处理完成(即收到了checkpointing信号)后向checkpoint coordinator上报自身checkpoint完成信息
     *    3. checkpointing coordinator收到所有算子任务的checkpointing完成信息后再向各算子任务广播本次checkpointing全局完成信息
     *    4. 两阶段事务提交sink算子收到checkpointing coordinator的回调信息时执行事务commit操作(数据真正被发出)
     *    5. 提交阶段(第二阶段提交事务)如果事务提交未成功 则重启之后会检查所有未提交的事务重新提交
     *    6. 如果目标系统不支持事务 sink算子可以采用预写日志的方式输出数据(在ck全部完成之前先将要输出的数据存到状态里等到ck全部完成之后才一次性输出到目标系统)
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setParallelism(2);
		
		DataStreamSource<Event> dataStreamSource = env.addSource(new TestRichCusomSource());
		
		// RedisSink需要一个FlinkJedisPoolConfig参数配置redis服务
		FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
		RedisSink<Event> redisSink = new RedisSink<Event>(config, new RedisInsertMapper());
		dataStreamSource.addSink(redisSink);
		
		env.execute();

	}

}
