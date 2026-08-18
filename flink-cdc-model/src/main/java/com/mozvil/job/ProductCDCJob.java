package com.mozvil.job;

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProductCDCJob {

    public static void main(String[] args) {
        // 本地Standalone模式下运行Flink Job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 启用checkpointing with a 1-minute interval
        env.enableCheckpointing(60000);
        // Set the parallelism of the Flink job to 1 for simplicity
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Define the source for CDC data from MySQL
        String cdcSourceDDL = "CREATE TABLE t_provider (" +
                "id INTEGER NOT NULL, " +
                "name VARCHAR(32), " +
                "description VARCHAR(64), " +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mysql-cdc', " +
                "'hostname' = '192.168.247.162', " +
                "'port' = '3306', " +
                "'username' = 'root', " +
                "'password' = '123456', " +
                "'database-name' = 'zhe_db', " +
                // 设置scan.startup.mode为earliest-offset以从最早的偏移量开始消费数据
                "'scan.startup.mode' = 'earliest-offset', " +
                "'table-name' = 't_provider')";
        tableEnv.executeSql(cdcSourceDDL);

        String kafkaSinkDDL = "CREATE TABLE kafka_sink (" +
                "id INTEGER NOT NULL, " +
                "name VARCHAR(32), " +
                "description VARCHAR(64), " +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'upsert-kafka', " +
                "'topic' = 'product-cdc', " +
                "'properties.bootstrap.servers' = '192.168.247.162:9192', " +
                "'key.format' = 'json', " +
                "'value.format' = 'json')";
        tableEnv.executeSql(kafkaSinkDDL);

        Table productTable = tableEnv.sqlQuery("select * from t_provider");
        productTable.executeInsert("kafka_sink");
    }

    // docker exec -it kafka-1
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --property print.key=true --topic product-cdc
    // MySQL8.4以后取消了SHOW MASTER STATUS的使用. 更改为SHOW BINARY LOG STATUS;
    // flink-connector-mysql-cdc.jar(3.5.0) org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils
    // line 121:  final String showMasterStmt = "SHOW MASTER STATUS"; -- sql syntax error

}
