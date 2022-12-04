package com.mozvil.sink;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import com.alibaba.fastjson.JSON;
import com.mozvil.demo.Event;
import com.mozvil.demo.TestRichCusomSource;
import com.mozvil.demo.avro.schema.AvroEventBean;

public class FileSinkDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 开启CheckPoint(2秒检查
		env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setParallelism(2);
		
		DataStreamSource<Event> dataStreamSource = env.addSource(new TestRichCusomSource()).setParallelism(2);
		
		// 当需要将流数据计算结果交给批计算工具(如hive和spark)做后续离线计算时需要将流计算结果输出到文件
		// 因为离线计算工具的数据输入是以文件形式为主的
		// 当需要输出到HDFS路径下的文件时路径的写法应该是: hdfs://hadoop100:8090/test/
		
		// 输出到文件(旧版本) 缺点：不会停止 文件无限增大； 一个subtask只能输出一个文件 无法合并；
		//dataStreamSource.writeAsText("D:/sink-test", WriteMode.OVERWRITE);
		
		// 使用StreamFileSink输出到文件(分桶bucket 可以按时间分桶)
		// StreamFileSink整合了checkpoint机制来保证Exactly Once语义 它还支持列式存储的写入格式 功能较强大
		// StreamFileSink输出的文件有3个生命周期状态：
		//     In-Progress File-正在写入中的文件
		//     Pending File-校验中(checkpoint)的文件
		//     Finished File-校验通过正式落地的文件
		
		// 构造一个FileSink
		//      forRowFormat()按行序列化输出到文件(逐行写入文件)
		/*
		FileSink<String> rowSink = FileSink.forRowFormat(new Path("D:/flinkSink/rowsink/"), new SimpleStringEncoder<String>("utf-8"))
		        // 设置文件写入策略
		        .withRollingPolicy(DefaultRollingPolicy.builder()
		        		// 写入持续10秒时间
		        		.withRolloverInterval(Duration.ofSeconds(10L))
		        		// 文件最大为1G
		        		.withMaxPartSize(new MemorySize(1024 * 1024 * 1024))
		        		.build())
		        // 设置分桶策略 分桶后每个桶都有一个对应的子目录生成(在前面传入的D:/filesink/路径下)
		        .withBucketAssigner(new DateTimeBucketAssigner<String>())
		        // 设置桶对应目录检查的时间间隔
		        .withBucketCheckInterval(5)
		        // 输出文件设置
		        .withOutputFileConfig(OutputFileConfig.builder()
		        		// 文件名加前缀
		        		.withPartPrefix("mozvil")
		        		// 文件名后缀
		        		.withPartSuffix(".txt")
		        		.build())
		        .build();
		dataStreamSource.map(JSON::toJSONString).sinkTo(rowSink);
		*/
		
		//      forBulkFormat()按桶序列化输出到文件(一次性全部写入文件) 支持列式存储格式(在数仓中列式存储的格式使用较多 因为经常需要查询某一些列而不需要整行数据 这样性能更高)
		// 方式1: 使用avro的SchemaBuilder直接生成avro的Schema 用以生成一个ParquetWriterFactory
		/*
		Schema schema = SchemaBuilder.builder()
				.record("DataRecord")
				.namespace("")
				.doc("用户行为测试数据")
				.fields()
				.requiredString("user")
				.requiredLong("timestamp")
				.requiredString("eventId")
				// 构建eventInfo字段
				.name("eventInfo")
				.type()
				.map()
				.values()
				.type("string")
				.noDefault()
				// 结束构建
				.endRecord();
		ParquetWriterFactory<GenericRecord> writerFactory0 = AvroParquetWriters.forGenericRecord(schema);
		FileSink<GenericRecord> parquetSink0 = FileSink
				.forBulkFormat(new Path("D:/flinkSink/bulksink/"), writerFactory0)
				// bulk模式下的文件滚动策略只有一种(当checkpoint发生时文件开始滚动)
				.withRollingPolicy(OnCheckpointRollingPolicy.build())
				// 按时间分桶(可以通过参数指定分桶目录名的时间格式)
				.withBucketAssigner(new DateTimeBucketAssigner<GenericRecord>("yyyy-MM-dd-HH"))
				.withBucketCheckInterval(5)
				.withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("mozvil").withPartSuffix(".parquet").build())
				.build();
		
		// 将输入流中数据类型Event转成GenericRecord类型
		// GenericRecord并没有实现JDK的Serializable序列化器，所以此处需要通过一个GenericRecordAvroTypeInfo指定返回类型(avro的序列化器)
		// 由于returns方法只作用在lambda表达式上这里不能使用匿名类实现的MapFunction
		dataStreamSource.map((MapFunction<Event, GenericRecord>) event -> {
			GenericData.Record record = new GenericData.Record(schema);
			record.put("user", event.getUser());
			record.put("timestamp", event.getTimestamp());
			record.put("eventId", event.getEventId());
			record.put("eventInfo", event.getEventInfo());
			return record;
		}).returns(new GenericRecordAvroTypeInfo(schema)).sinkTo(parquetSink0);
		*/
		
		// 方式2: AvroParquetWriters的forSpecificRecord()方法可以从上面生成的schema类通过反射获取schema并生成一个指定泛型的ParquetWriterFactory
		//       parquet格式文件需要schema avro(avro.apache.org/docs/)提供了schema类的编译插件(avro-maven-plugin) 见pom.xml
		//       将parquet格式描述文件avsc放置于src/main/resources/路径下 并配置vro-maven-plugin的sourceDirectory为${project.basedir}/src/main/resources/(avro自动搜索此目录下的所有格式描述文件)
		//       avro-maven-plugin的targetDirectory为schema类输出的跟路径
		/*
		ParquetWriterFactory<AvroEventBean> writerFactory1 = AvroParquetWriters.forSpecificRecord(AvroEventBean.class);
		FileSink<AvroEventBean> parquetSink1 = FileSink
				.forBulkFormat(new Path("D:/flinkSink/bulksink/"), writerFactory1)
				// bulk模式下的文件滚动策略只有一种(当checkpoint发生时文件开始滚动)
				.withRollingPolicy(OnCheckpointRollingPolicy.build())
				.withBucketAssigner(new DateTimeBucketAssigner<AvroEventBean>())
				.withBucketCheckInterval(5)
				.withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("mozvil").withPartSuffix(".parquet").build())
				.build();
		
		dataStreamSource.map(event -> {
			Map<CharSequence, CharSequence> eventInfo = new HashMap<CharSequence, CharSequence>();
			for(Map.Entry<String, String> entry : event.getEventInfo().entrySet()) {
				eventInfo.put(entry.getKey(), entry.getValue());
			}
			return new AvroEventBean(event.getUser(), event.getEventId(), event.getTimestamp(), eventInfo);
		}).returns(AvroEventBean.class)
		.sinkTo(parquetSink1);
		*/
		
		// 方式3: 最简单方式：用Avro的规范Bean对象通过反射生成ParquetAvroWriterFactory
		// forReflectRecord()只需要传入一个自定义的实体类class即可
		ParquetWriterFactory<Event> writerFactory2 = AvroParquetWriters.forReflectRecord(Event.class);
		FileSink<Event> parquetSink2 = FileSink
				.forBulkFormat(new Path("D:/flinkSink/bulksink/"), writerFactory2)
				// bulk模式下的文件滚动策略只有一种(当checkpoint发生时文件开始滚动)
				.withRollingPolicy(OnCheckpointRollingPolicy.build())
				.withBucketAssigner(new DateTimeBucketAssigner<Event>())
				.withBucketCheckInterval(5)
				.withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("mozvil").withPartSuffix(".parquet").build())
				.build();
		dataStreamSource.sinkTo(parquetSink2);
		
		env.execute();
	}

}
