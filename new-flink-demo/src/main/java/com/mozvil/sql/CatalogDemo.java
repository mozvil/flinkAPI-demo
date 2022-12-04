package com.mozvil.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class CatalogDemo {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSettings);
		
		/**
		 * 要使用hive元数据管理空间需要引入flink-sql-connect-hive依赖(flink.apache.org官网下载对应的依赖版本)
		 * 本地环境将flink-sql-connect-hive jar包放在工程目录下的lib包中并将jar包引入classpath中
		 * 集群运行需要将jar包拷贝到flink路径下
		 */
		Catalog hiveCatalog = new HiveCatalog("hive", "default", "D:/dev/HadoopWS/new-flink-demo/src/main/resources");
		tableEnv.registerCatalog("hive_catalog", hiveCatalog);
		/**
		 * 要在hive下建表需要有权限操作HDFS 本地环境用户无权限操作HDFS 需要配置HADOOP HDFS的访问权限禁用权限
		 * 在Hadoop安装路径下编辑/opt/module/hadoop-3.3.4/etc/hadoop/hdfs-site.xml
		 * 增加配置(让所有用户访问hdfs)：
		 * <property>
         *     <name>dfs.permissions.enabled</name>
         *    <value>false</value>
         * </property>
		 */
		// 在hive的元数据空间中建连接类型是kafka的表后 在hive客户端里可以看到此表存在但无法使用hive查看表的结构和表数据
		// 用create temporary table创建的表并不在hiveCatalog中维护 所以hive客户端show tables将看不到这张表
		// flink的CatalogManager用一个temporaryTables的HashMap存放临时表
		/*
		tableEnv.executeSql(
				"CREATE TABLE `hive_catalog`.`default`.t_kafka (" +
				"    id int, " +
				"    name string, " +
				"    age int, " +
				"    gender string" +
				") WITH (" +
				"    'connector' = 'kafka', " + 
				"    'topic' = 'flinksql-1', " +
				"    'properties.bootstrap.servers' = 'hadoop100:9092', " +
				"    'properties.group.id' = 'testGroup', " +
				"    'format' = 'json', " +
				"    'scan.startup.mode' = 'earliest-offset', " +
				"    'json.fail-on-missing-field' = 'false', " +
				"    'json.ignore-parse-errors' = 'true'" +
				")"
		);
		tableEnv.executeSql("create view `hive_catalog`.`default`.t_kafka_view as select id, name, age from `hive_catalog`.`default`.t_kafka");
		*/
		tableEnv.executeSql("select * from `hive_catalog`.`default`.t_kafka").print();
		
		tableEnv.executeSql("show catalogs").print();
		tableEnv.executeSql("use catalog default_catalog");
		tableEnv.executeSql("show databases").print();
		tableEnv.executeSql("use default_database");
		tableEnv.executeSql("show tables").print();

		tableEnv.executeSql("use catalog hive_catalog");
		tableEnv.executeSql("show databases").print();
		tableEnv.executeSql("use `default`");
		tableEnv.executeSql("show tables").print();


	}

}
