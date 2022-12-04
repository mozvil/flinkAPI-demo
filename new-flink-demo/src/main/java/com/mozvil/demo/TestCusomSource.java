package com.mozvil.demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;

/**
 * 测试自定义SourceFunction类
 * @author Jeff Home
 *
 */
public class TestCusomSource {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		
		//DataStreamSource<Event> dataStreamSource = env.addSource(new TestSourceFunction());
		// RichSourceFunction 可以记录状态
		DataStreamSource<Event> dataStreamSource = env.addSource(new TestRichCusomSource()).setParallelism(2);
		SingleOutputStreamOperator<String> result = dataStreamSource.map(JSON::toJSONString);
		result.print();
		
		env.execute();
	}

}
