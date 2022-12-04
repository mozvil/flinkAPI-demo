package com.mozvil.sql.cdc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCDCDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointStorage("file:///d:/flinkSink/ckpt");
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		// 创建cdc连接器source表
		tableEnv.executeSql(
				"CREATE TABLE t_score (" +
				"    id INT, " +
				"    name STRING, " +
				"    gender STRING, " +
				"    score DOUBLE, " +
				"    PRIMARY KEY(id) NOT ENFORCED" +
				") WITH (" +
				"    'connector' = 'mysql-cdc', " +
				"    'hostname' = '192.168.20.130', " +
				"    'port' = '3306', " +
				"    'username' = 'root', " +
				"    'password' = 'QAZwsx12345678', " +
				"    'database-name' = 'flink_test', " +
				"    'table-name' = 't_score'" +
				")"
		);
		
		// 创建目标表(sink表)存放查询结果：每一个种gender中sum(score)最大的两个name
		// mysql建表t_score_rank(gender varchar, name varchar, score_amt double, rn bigint) gender和rn作联合主键
		tableEnv.executeSql(
				"create table t_score_rank (" +
				"    gender string, " +
				"    name string " +
				"    score_amt double, " +
				"    rn bigint, " +
				"    primary key(gender, rn) not enforced" +
				") with (" +
				"    'connector' = 'jdbc', " +
				"    'driver' = 'com.mysql.cj.jdbc.Driver', " +
				"    'url' = 'jdbc:mysql://192.168.20.130:3306/flink_test', " +
				"    'table-name' = 't_score_rank', " +
				"    'username' = 'root', " +
				"    'password' = 'QAZwsx12345678'" +
				")"
		);
		
		tableEnv.executeSql(
				"insert into t_score_rank " +
				"    select gender, name, score_amt, rn " + 
				"    from (" + 
				"        select gender, name, score_amt, " + 
				"               row_number() over (partition by gender order by score_amt desc) as rn " +
				"        from (" +
				"            select gender, name, sum(score) as score_amt " +
				"            from t_score" +
				"            group by gender, name " +
				"        ) o1 " + 
				"    ) o2 " +
				"    where rn <= 2"
		);

	}

}
