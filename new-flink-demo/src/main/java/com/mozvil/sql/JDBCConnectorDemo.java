package com.mozvil.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JDBCConnectorDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		// 建表映射mysql中的表(在mysql中建表id int primary, name varchar, age int, gender varchar)
		// 扫描模式(scan source)读取数据 只读一次(有界流 批模式 读完即止)
		// 这种jdbc-connector模式在流处理场景下通常被用来作为sink输出到其他数据库表 
		// 即上游数据经过各种算子处理(分组计算) 将计算结果存入数据库(中间会有数据更新)
		tableEnv.executeSql(
				"create table t_test (" +
				"    id int primary key, " + 
				"    name string, " + 
				"    age int, " +
				"    gender string " +
				") with (" +
				"    'connector' = 'jdbc', " +
				"    'url' = 'jdbc:mysql://192.168.20.130:3306/flink_test', " +
				"    'table-name' = 'stu', " +
				"    'username' = 'root', " +
				"    'password' = 'QAZwsx12345678'" +
				")"
		);
		
		tableEnv.executeSql("sele t * from t_test").print();
		
		// CDC连接器可以捕获数据库中持续不断变化的表中数据(流模式 支持变化语义 +I -U +U -D等)

	}

}
