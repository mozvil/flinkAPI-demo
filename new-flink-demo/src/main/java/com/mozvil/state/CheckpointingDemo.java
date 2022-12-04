package com.mozvil.state;

import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointingDemo {

	/**
	 * Checkpointing机制的存在是为了在Task失败或奔溃时各算子可以恢复状态存储中的数据并保持一致性的语义
	 * Checkpointing通过在数据流中插入标记数据(barrier)来记录每个算子处理数据的进度(在遇到标记数据时生成状态数据快照 确保标记数据前的数据都已经被当前算子处理过)
	 * 当所有算子都接收到标记说明都已经处理过同一批数据 这样这些算子生成的快照数据拥有一致性的数据逻辑
	 * 当系统发生故障重启之后只需要获取最近一次快照的状态存储数据 然后从此次生成快照时的标记后数据开始处理即可
	 * 重启后source算子需要按照快照记录的状态数据查找之前读取数据的偏移量(有些数据需要重放)
	 * checkpointing由JobManager发起 同一个checkpoint标记必须被所有算子全部接收并返回才能作为一个有效的快照被保存
	 */
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.setParallelism(1);
		
		// 开启checkpointing 参数#1：每两次checkpointing之间间隔毫秒数
		env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
		
		// 指定checkpointing数据的存储地址(文件系统路径)
		env.getCheckpointConfig().setCheckpointStorage(new Path(""));
		
		// 允许checkpointing失败的最大次数(checkpointing失败超过最大次数Job失败)
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
		
		// checkpointing的算法模式(是否需要对齐 默认是EXACTLY_ONCE模式 非对齐的选项就是AT_LEAST_ONCE)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		
		// job取消时是否保留最后一次checkpointing数据(Task重启快照数据自动会保留 Job暂停需要保留快照需要配置这个参数)
		env.getCheckpointConfig().setExternalizedCheckpointCleanup(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		
		// 设置checkpointing对齐的超时时间(如果超时则视为本次checkpointing失败 checkpointing失败次数超过上线则Job失败)
		env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofMillis(2000));
		
		// 两次checkpointing的最小间隔时间(为了防止两次checkpointing的时间间隔太短)
		env.getCheckpointConfig().setCheckpointInterval(2000);
		
		// 最大并行的checkpoingting数量(没有完成的或正在进行的checkpointing最大数量)
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
		
		// 设置两次ck之间的最小时间间隔(为了防止ck过多占用资源)
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
		
		// 设置(一个算子的)checkpointing的超时时间
		env.getCheckpointConfig().setCheckpointTimeout(3000);
		
		// 设置状态后端的实现(默认是HashMapStateBackend)
		env.setStateBackend(new EmbeddedRocksDBStateBackend());

	}

}
