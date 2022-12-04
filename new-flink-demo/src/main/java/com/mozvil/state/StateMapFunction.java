package com.mozvil.state;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * 算子的状态使用必须实现CheckpointedFunction接口并实现initializeState方法
 * @author Jeff Home
 *
 */
public class StateMapFunction implements MapFunction<String, String>, CheckpointedFunction {

	private static final long serialVersionUID = -5446698671370887230L;
	
	private ListState<String> listState;
	
	/**
	 * MapFunction实现方法
	 */
	@Override
	public String map(String value) throws Exception {
		// 为了测试Task失败后重启状态数据是否保存 这里让map算子有一定机率得抛出异常
		if(value.equals("x") && RandomUtils.nextInt(1, 15) % 4 == 0) {
			throw new Exception("Task goes wrong!!!!");
		}
		// 往状态中添加数据
		listState.add(value);
		// 拼接所有状态中的历史数据
		Iterable<String> strings = listState.get();
		StringBuilder sb = new StringBuilder();
		for(String s : strings) {
			sb.append(s);
		}
		return sb.toString();
	}
	
	/**
	 * 系统对状态数据作快照(持久化)时调用此方法 可以用此方法在持久化之前对状态数据做一些操控
	 */
	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		//System.out.println("Checkpoint invoked " + context.getCheckpointId() + "times");
	}

	/**
	 * 算子任务在启动之初会调用此方法 为用户进行状态数据的初始化
	 */
	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// 获取算子状态存储管理器
		OperatorStateStore store = context.getOperatorStateStore();
		// 算子状态存储管理器只提供List数据结构存储数据
		// 定义一个算子状态存储数据结构的描述 参数1：描述名称 参数2：泛型类型
		ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<String>("strings", String.class);
		// getListState()方法会自动加载task重启之前所持久化的状态数据
		// 如果job重启了则不会自动加载此前的快照状态数据(但是可以通过设置让Flink加载快照数据)
		// getListState() Task失败重启之后获取到快照的模式为roundRobin(按照新的实例数量平均分配 轮询)
		listState = store.getListState(listStateDescriptor);
		// getUnionListState() Task失败重启之后所有实例将获取到重启之前的所有实例中的状态数据(广播模式 从快照中获取)
		//listState = store.getUnionListState(listStateDescriptor);
	}

}
