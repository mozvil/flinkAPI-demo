package com.mozvil.sql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson2.JSON;

public class StreamWatermarkDemo {
	
	/**
	 * 
	 * {"guid":1,"eventId":"e02","eventTime":"2022-11-29 17:20:35.300","pageId":"p001"}
	 * {"guid":2,"eventId":"e03","eventTime":"2022-11-29 17:22:38.590","pageId":"p003"}
	 * {"guid":3,"eventId":"e02","eventTime":"2022-11-29 17:26:43.740","pageId":"p002"}
	 * {"guid":4,"eventId":"e05","eventTime":"2022-11-29 17:31:29.610","pageId":"p005"}
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		DataStreamSource<String> sourceStream = env.socketTextStream("hadoop100", 9998);
		SingleOutputStreamOperator<EventInfo> stream = 
				sourceStream.map(s -> JSON.parseObject(s, EventInfo.class))
				            .assignTimestampsAndWatermarks(
				            		WatermarkStrategy.<EventInfo>forMonotonousTimestamps()
				            		                 .withTimestampAssigner(new SerializableTimestampAssigner<EventInfo>() {

														private static final long serialVersionUID = 3825221718506647908L;

														@Override
														public long extractTimestamp(EventInfo element, long recordTimestamp) {
															return element.eventTime;
														}
				            		                	 
				            		                 })
				);
		
		// 查看数据流的watermark
		/*
		stream.process(new ProcessFunction<EventInfo, String>() {

			private static final long serialVersionUID = -7876232767807746481L;

			@Override
			public void processElement(EventInfo element, ProcessFunction<EventInfo, String>.Context context,
					Collector<String> out) throws Exception {
				long watermark = context.timerService().currentWatermark();
				out.collect(element + " [watermark: " + watermark + "]");
				
			}
			
		}).print();
		*/
		// 流转表过程中如果不显示指定watermark字段 则流中的watermark会丢失 需要指定schema
		tableEnv.createTemporaryView("t_events", stream, Schema.newBuilder()
			      .column("guid", DataTypes.INT())
			      .column("eventId", DataTypes.STRING())
			      .column("eventTime", DataTypes.BIGINT())
			      .column("pageId", DataTypes.STRING())
			      // 重新定义表上的watermark策略
			      /*
			      .columnByExpression("rt", "to_timestamp_ltz(eventTime, 3)")
			      .watermark("rt", "rt - interval '1' second")
			      */
			      // 延用流上的watermark策略
			      // 连接器暴露的rowtime元数据(流中的watermark)
			      .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
			      // 声明表的watermark沿用流中的watermark
			      .watermark("rt", "source_watermark()")
			      .build());
		
		tableEnv.executeSql("select guid, eventId, eventTime, pageId, rt, current_watermark(rt) as wm from t_events").print();
		
		// 表转成流时watermart会自动沿用
		
		
		env.execute();
	}

}
