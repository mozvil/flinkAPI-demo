package com.mozvil.tolerance;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TaskFailoverDemo {

	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		configuration.setInteger("rest.port", 8081);
		// 指定从某个savepoint恢复状态
		configuration.setString("execution.savepoint.path", "file:///D:/checkpoint/7ecbd4f9106957c42109bcde/chk-544");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		
		/**
		 * task失败自动重启策略配置
		 */
		RestartStrategies.RestartStrategyConfiguration restartStrategy;
		
		// 默认的故障重启策略：不重启(只要有一个task失败整个Job就失败了)
		restartStrategy = RestartStrategies.noRestart();
		
		// 固定次数和延迟重启策略 参数#1：故障重启的最大次数(超出则Task和Job都失败) 参数#2：两次重启之间的延迟间隔
		restartStrategy = RestartStrategies.fixedDelayRestart(5, 2000);
		
		/**
		 * 带惩罚的延迟重启策略(每次重启的时间间隔比前一次重启时间间隔都长 如：第一次重启间隔1秒 第二次2秒 第三次4秒 第四次8秒 ...)
		 * 参数说明：
		 *      参数#1(initialBackoff)         - 重启时间间隔时长的初始值(第一次重启)
		 *      参数#2(maxBackoff)             - 重启时间间隔最大惩罚时长(重启多次后惩罚的时间间隔将增大到这个固定值)
		 *      参数#3(backoffMultiplier)      - 重启时间间隔的惩罚倍数(每多故障一次重启延迟惩罚就在前一次的惩罚时长上*这个倍数)
		 *      参数#4(resetBackoffThreshold)  - 重置惩罚时长的平稳运行时长阈值(平稳运行达到这个阈值后 如果再故障则故障重启延迟时间将重置为初始值initialBackoff)
		 *      参数#5(jitterFactor)           - 重启延迟抖动时长(在已惩罚的重启时间间隔上再加上一个百分比随机数即抖动时长) 这样可以防止集群在同一个时间点上重启多个Task
		 */
		restartStrategy = RestartStrategies.exponentialDelayRestart(Time.seconds(1), Time.seconds(60), 2.0D, Time.seconds(200), 2);
		
		/**
		 * 按失败率重启(只要符合低于预期的失败率可以无限次数重启)
		 * 参数#1(failureRate)      - 在指定时长(failureInterval)内的最大失败次数(超过则Task和Job失败)
		 * 参数#2(failureInterval)  - 指定时长
		 * 参数#3(delayInteral)     - 两次重启之间的时间间隔
		 */
		restartStrategy = RestartStrategies.failureRateRestart(2, Time.minutes(60), Time.minutes(1));
		
		// 如果在flink-conf.yaml文件中自定义了重启策略 可以退回到配置文件所配置的自定义策略
		restartStrategy = RestartStrategies.fallBackRestart();
		
		/**
		 * Failover-strategy策略(jobmanager.execution.failover-strategy: region)
		 *        含义：当一个task发生故障需要重启时 是否重启整个Job中的所有task(Restart All Failover Strategy)还是只重启一部分task(Restart Pipelined Region Failover Strategy)
		 *        默认为Restart Pipelined Region Failover Strategy(当task失败时该策略会计算出需要重启的最小region集)
		 */
		
		env.setRestartStrategy(restartStrategy);
		
		// cluster级别的故障或人为重启恢复快照
		// # Default savepoint target directory
		// flink-conf.yaml中 state.savepoints.dir: hdfs:///flink/savepoints
		
		/**
		 * 手动触发savepoint命令
		 * # 触发一次savepoint
		 * $ /bin/flink savepoint :jobId [:targetDirectory]
		 * 
		 * # 为yarn模式集群触发savepoint
		 * $ /bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId
		 * 
		 * # 停止一个job集群并触发savepoint
		 * $ /bin/flink stop --savepointPath [:targetDirectory] :jobId
		 * 
		 * # 从一个指定的savepoint恢复启动job集群
		 * $ /bin/flink run -s :savepointPath [:runArgs]
		 * 
		 * # 删除一个savepoint
		 * $ /bin/flink savepoint -d :savepointPath
		 */
	}

}
