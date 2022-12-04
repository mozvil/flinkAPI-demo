package com.mozvil.transform;

import java.io.Serializable;
import java.util.List;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson2.JSON;

import lombok.Data;

public class TransformDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		env.setParallelism(1);
		DataStreamSource<String> dataStreamSource = env.readTextFile("D:/dev/HadoopWS/new-flink-demo/src/main/java/com/mozvil/demo/demo-data.txt");
		SingleOutputStreamOperator<UserInfo> beanStream = dataStreamSource.map(user -> JSON.parseObject(user, UserInfo.class));
		
		// 过滤friends数量为3的数据
		//beanStream = beanStream.filter(userInfo -> userInfo.getFriends().size() < 3);
		
		// 压扁数据(把每个UserInfo和下属的friends合并放入新的流)
		SingleOutputStreamOperator<UserFriend> flatMappedStream = beanStream.flatMap(new FlatMapFunction<UserInfo, UserFriend>() {

			private static final long serialVersionUID = 6435655121735883717L;

			@Override
			public void flatMap(UserInfo user, Collector<UserFriend> out) throws Exception {
				UserFriend userFriend;
				for(Friend friend : user.getFriends()) {
					userFriend = new UserFriend();
					userFriend.setUid(user.getUid());
					userFriend.setName(user.getName());
					userFriend.setGender(user.getGender());
					userFriend.setFid(friend.getFid());
					userFriend.setFname(friend.getName());
					out.collect(userFriend);
				}
			}
			
		});
		// male和female各自的总数
		SingleOutputStreamOperator<Tuple2<String, Integer>> groupSum = flatMappedStream.map(
						userFriend -> Tuple2.of(userFriend.getGender(), 1)
				).returns(new TypeHint<Tuple2<String, Integer>>() {});
		groupSum.keyBy(tp -> tp.f0).sum(1).print("分组求每组的总数");
		
		// male和female的UserInfo中各自friend数量最大值(带出那个拥有friends数量最多的UserInfo的其他字段)
		SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> groupMax = 
				beanStream.map(user -> 
					Tuple4.of(user.getUid(), user.getName(), user.getGender(), user.getFriends().size())
				).returns(new TypeHint<Tuple4<Integer, String, String, Integer>>() {});
		// 这里uid name两个非分组非聚合字段也一并查出返回(这两个字段值获取的是每个分组中第一条数据中相应字段的值 --> 滚动聚合)
		// 滚动聚合
		// max(): 
		//     在状态中保存第一条数元组中的所有元素，然后聚合过程中如果有符合条件(这里就是第4个元素值较大的)就用当前元组中第4个元素替换原来状态中保存的元组中的第4个元素
		//     不断刷新状态中保存的元组的第4个元素值直到获取到最大的那个，但是状态中的元组其他字段是不刷新的
		// maxBy(): 
		//     与上述过程一样，但是在替换状态中保存的元组时，所有元组中的元素是同时替换的
		groupMax.keyBy(tp -> tp.f2).maxBy(3).print("分组求每组的最大值");
		// min()和minBy()原理同上
		groupMax.keyBy(tp -> tp.f2).minBy(3).print("分组求每组的最小值");
		
		// 使用reduce算子实现上述需求
		// 不同点：如果有两个或以上UserInfo里的friends数量相同并且同为最多的，则要将状态中保存的UserInfo里的uid name字段值都更新为最近一个friends数量最多的UserInfo里的uid name字段值
		SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduceResult = groupMax.keyBy(tp -> tp.f2).reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {

			private static final long serialVersionUID = 6642452280395680347L;
			
			// ReduceFunction是自定义的滚动聚合算子 它的原理和上面max/min/maxBy/minBy等完全相同只是聚合的逻辑可以自己来实现
			// reduce()中的两个参数
			// --第一个参数是截至前一次聚合的结果
			// --第二个参数是当前新到的数据
			@Override
			public Tuple4<Integer, String, String, Integer> reduce(
					Tuple4<Integer, String, String, Integer> value1,
					Tuple4<Integer, String, String, Integer> value2) throws Exception {
				if(value1 == null || value2.f3 >= value1.f3) {
					return value2;
				} else {
					return value1;
				}
			}
			
		});
		reduceResult.print("Reduce算子计算结果: ");
		env.execute();
	}

}

@Data
class UserInfo implements Serializable {
	private static final long serialVersionUID = 7578764614083759273L;
	private int uid;
	private String name;
	private String gender;
	private List<Friend> friends;
}

@Data
class Friend implements Serializable {
	private static final long serialVersionUID = 49452490186561946L;
	private int fid;
	private String name;
}

@Data
class UserFriend implements Serializable {
	private static final long serialVersionUID = -2774927356574637650L;
	private int uid;
	private String name;
	private String gender;
	private int fid;
	private String fname;
}
