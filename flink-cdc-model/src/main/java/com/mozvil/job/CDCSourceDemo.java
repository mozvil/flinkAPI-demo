package com.mozvil.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.mozvil.pojo.RechargeInfo;
import com.mozvil.pojo.UserInfo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

public class CDCSourceDemo {

    public static final String DIM_PK_FIELD = "user_id";

    private static final String DELETE_MARKER = "__DELETED__";

    private static final ObjectMapper KAFKA_MAPPER = new ObjectMapper();

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final MapStateDescriptor<String, UserInfo> DIM_STATE =
            new MapStateDescriptor<>("dim-broadcast", String.class, UserInfo.class);


    /**
     * 解析 Flink CDC 默认 JSON:
     *     {"before":{...},"after":{"user_id":1,"username":"...","login_time":"...","status":1},"op":"r|c|u|d"}
     * - r / c(initial / create-insert) → after
     * - u(update) → after
     * - d(delete) → before 的主键，value=null（下游 remove）
     */
    public static class DimParseMap extends RichFlatMapFunction<String, Tuple2<String, UserInfo>> {

        @Serial
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(DimParseMap.class);

        private transient ObjectMapper mapper;

        private void initMapper() {
            if (this.mapper == null) {
                this.mapper = new ObjectMapper();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            initMapper();
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, UserInfo>> out) {
            initMapper();

            try {
                JsonNode root = mapper.readTree(value);
                // empty data to be thrown away
                if (root == null || root.isNull()) return;

                JsonNode dataNode = extractDataNode(root);
                String pkId = extractPrimaryKey(dataNode);
                // data without primary key to be thrown awy
                if (pkId == null) return;

                String op = extractOp(root);
                // 处理删除操作（构建墓碑对象，防止序列化 NPE）
                if ("d".equals(op)) {
                    UserInfo tombstone = new UserInfo();
                    tombstone.setUserId(Long.parseLong(pkId));
                    tombstone.setUsername(DELETE_MARKER);
                    out.collect(Tuple2.of(pkId, tombstone));
                    return;
                }

                // Insert/Update/Initial
                UserInfo userInfo = new UserInfo();
                userInfo.setUserId(Long.parseLong(pkId));
                userInfo.setUsername(dataNode.path("username").asText(null));
                userInfo.setLoginTime(parseDate(dataNode.path("login_time")));
                userInfo.setPhoneNum(dataNode.path("phone_num").asText(null));
                userInfo.setStatus(dataNode.path("status").asInt(0));
                out.collect(Tuple2.of(pkId, userInfo));
            } catch (Exception e) {
                LOG.error("Failed to parse dimension CDC data: {}", value, e);
            }
        }

        private JsonNode extractDataNode(JsonNode root) {
            if (root.has("after") && !root.get("after").isNull())
                return root.get("after");
            if (root.has("before") && !root.get("before").isNull())
                return root.get("before");
            return root;
        }

        private String extractPrimaryKey(JsonNode dataNode) {
            if (dataNode != null && dataNode.has(DIM_PK_FIELD) && !dataNode.get(DIM_PK_FIELD).isNull()) {
                return dataNode.get(DIM_PK_FIELD).asText();
            }
            return null;
        }

        private String extractOp(JsonNode root) {
            if (root.has("op"))
                return root.get("op").asText("c");
            return "c";
        }

        private Date parseDate(JsonNode node) {
            if (node == null || node.isMissingNode() || node.isNull()) return null;
            if (node.isNumber()) return new Date(node.asLong());
            try {
                return Timestamp.valueOf(LocalDateTime.parse(node.asText(), DATE_FORMATTER));
            } catch (Exception e) {
                LOG.warn("Failed to parse date: {}", node.asText());
                return null;
            }
        }
    }

    /**
     * 解析事实表 recharge_info 的 CDC JSON，输出部分填充的 RechargeInfo：
     * userInfo 中只设 userId（外键），其余字段待广播流补全。
     */
    public static class ActionParseMap extends RichFlatMapFunction<String, RechargeInfo> {

        @Serial
        private static final long serialVersionUID = 1L;

        private static final Logger LOG = LoggerFactory.getLogger(ActionParseMap.class);

        private transient ObjectMapper mapper;

        private void initMapper() {
            if (this.mapper == null) {
                this.mapper = new ObjectMapper();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            initMapper();
        }

        @Override
        public void flatMap(String value, Collector<RechargeInfo> out) {
            initMapper();

            try {
                JsonNode root = mapper.readTree(value);
                JsonNode row = extractDataNode(root);
                if (row == null || row.isNull()) return;

                RechargeInfo info = new RechargeInfo();
                JsonNode userIdNode = row.get(DIM_PK_FIELD);
                if (userIdNode != null && !userIdNode.isNull()) {
                    UserInfo userInfoRef = new UserInfo();
                    userInfoRef.setUserId(userIdNode.asLong());
                    info.setUserInfo(userInfoRef);
                }
                info.setPrice(getInt(row, "price"));
                info.setActionTime(parseDate(row.path("action_time")));
                info.setPayMethod(getInt(row, "pay_method"));
                info.setRemark(row.path("remark").asText(null));

                out.collect(info);
            } catch (Exception e) {
                LOG.error("Failed to parse action CDC data: {}", value, e);
            }
        }

        private JsonNode extractDataNode(JsonNode root) {
            if (root.has("after") && !root.get("after").isNull())
                return root.get("after");
            if (root.has("before") && !root.get("before").isNull())
                return root.get("before");
            return root;
        }

        private Integer getInt(JsonNode parent, String field) {
            JsonNode node = parent.path(field);
            return (node.isMissingNode() || node.isNull()) ? null : node.asInt();
        }

        private Date parseDate(JsonNode node) {
            if (node == null || node.isMissingNode() || node.isNull()) return null;
            if (node.isNumber()) return
                    new Date(node.asLong());
            try {
                return Timestamp.valueOf(LocalDateTime.parse(node.asText(), DATE_FORMATTER));
            } catch (Exception e) {
                LOG.warn("Failed to parse date: {}", node.asText());
                return null;
            }
        }
    }

    /**
     * 事实流 connect 维度广播流：用外键 user_id 查广播状态补全 UserInfo。
     * 若维度尚未到位，则 userInfo 仅含 userId（下游可据此判断是否命中维度）。
     */
    public static class EnrichWithDimFunction
            extends BroadcastProcessFunction<RechargeInfo, Tuple2<String, UserInfo>, RechargeInfo> {

        @Serial
        private static final long serialVersionUID = 1L;

        private static final Logger LOG = LoggerFactory.getLogger(EnrichWithDimFunction.class);


        @Override
        public void processElement(RechargeInfo factValue, ReadOnlyContext ctx, Collector<RechargeInfo> out) throws Exception {
            if (factValue == null || factValue.getUserInfo() == null || factValue.getUserInfo().getUserId() == null) {
                // incompleted data to be thrown away
                return;
            }

            ReadOnlyBroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(DIM_STATE);
            String userIdKey = String.valueOf(factValue.getUserInfo().getUserId());
            UserInfo dimInfo = broadcastState.get(userIdKey);
            if (dimInfo != null) {
                factValue.setUserInfo(dimInfo);
            }
            out.collect(factValue);
        }

        @Override
        public void processBroadcastElement(Tuple2<String, UserInfo> dimValue, Context ctx, Collector<RechargeInfo> out) throws Exception {
            if (dimValue == null || dimValue.f0 == null || dimValue.f1 == null) {
                return;
            }

            String key = dimValue.f0;
            UserInfo value = dimValue.f1;
            if (DELETE_MARKER.equals(value.getUsername())) {
                ctx.getBroadcastState(DIM_STATE).remove(key);
                LOG.debug("Removed dimension from broadcast state: {}", key);
            } else {
                ctx.getBroadcastState(DIM_STATE).put(key, value);
                LOG.debug("Updated dimension in broadcast state: {}", key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///opt/flink/checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        // 启用checkpointing with a 1-minute interval
        env.enableCheckpointing(50000);
        // Set the parallelism of the Flink job to 1 for simplicity
        env.setParallelism(2);

        // #1 Dim Data for broadcast
        MySqlSource<String> dimSource = MySqlSource.<String>builder()
                .hostname("192.168.247.162")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("moz_flink_test")
                .tableList("moz_flink_test.user_info")
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        BroadcastStream<Tuple2<String, UserInfo>> broadcastDim = env
                .fromSource(dimSource, WatermarkStrategy.noWatermarks(), "mysql-dim-source")
                .flatMap(new DimParseMap())
                .name("parse-dim")
                .uid("dim-parsed")
                .broadcast(DIM_STATE);

        // #2 Action Data Stream
        MySqlSource<String> actionSource = MySqlSource.<String>builder()
                .hostname("192.168.247.162")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("moz_flink_test")
                .tableList("moz_flink_test.recharge_info")
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // #3 Connect to dim (broadcast)
        DataStream<RechargeInfo> parsedAction = env
                .fromSource(actionSource, WatermarkStrategy.noWatermarks(), "mysql-action-source")
                .flatMap(new ActionParseMap())
                .name("parse-action")
                .uid("action-parsed");

        DataStream<RechargeInfo> enrichedAction = parsedAction
                .connect(broadcastDim)
                .process(new EnrichWithDimFunction())
                .name("enrich-with-dim")
                .uid("action-enriched");

        // #4 Kafka Sink
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "192.168.247.162:9292,192.168.247.162:9192,192.168.247.162:9392");
        kafkaProps.setProperty("acks", "all");
        kafkaProps.setProperty("retries", "5");
        //kafkaProps.setProperty("retry.backoff.ms", "1000");
        //kafkaProps.setProperty("max.block.ms", "5000");

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.247.162:9292,192.168.247.162:9192,192.168.247.162:9392")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink_recharge_enriched")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(kafkaProps)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        enrichedAction
                .map(KAFKA_MAPPER::writeValueAsString)
                .sinkTo(kafkaSink)
                .name("kafka-sink")
                .uid("kafka-sink");

        env.execute("UserRechargeSyncJob");
    }

}
