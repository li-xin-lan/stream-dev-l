package com.stream.realtime.lululemon.practice;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.KafkaUtils;
import com.stream.realtime.lululemon.practice.func.*;
import com.stream.realtime.lululemon.practice.utils.WaterMarkUtils;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

import java.util.Properties;

public class FlinkWriteUserProfileToDoris {

    private static final String KAFKA_SERVERS = "172.22.78.0:9092";
    private static final String TOPIC_LOG = "realtime_v3_logs";
    private static final String TOPIC_USER = "flink-output-user-info";
    private static final String ORDER_COMMENT_OUTPUT_TOPIC="finalstream-to-kafka-fordoris";
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        EnvironmentSettingUtils.defaultParameter(env);

        // ------------------------------------------------------------------------------------------
        // 1) SQLServer CDC 评论流（commentDs）
        // ------------------------------------------------------------------------------------------
        Properties prop = new Properties();
        prop.put("snapshot.mode", "initial");

        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("localhost")
                .port(1433)
                .username("SA")
                .password("Lxl200538,./")
                .database("realtime_v3")
                .tableList("dbo.order_comment")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStream<JSONObject> commentDs =
                env.addSource(sqlServerSource, "sqlserver-comment")
                        .map(JSON::parseObject)
                        .map(new MapSqlServerData())  // 你自己封装的格式化函数
                        .uid("comment_format");
//        commentDs.print();

        // ------------------------------------------------------------------------------------------
        // 2) Kafka 日志流（kafkaLogDs）— 包含 keywords
        // ------------------------------------------------------------------------------------------
        KafkaSource<String> kafkaLogSource = KafkaUtils.buildKafkaSource(
                KAFKA_SERVERS,
                TOPIC_LOG,
                "flink_join_group",
                OffsetsInitializer.earliest()
        );

        DataStream<JSONObject> logDs =
                env.fromSource(
                                kafkaLogSource,
                                WaterMarkUtils.publicAssignWatermarkStrategyUseGsonParse("ts", 5),
                                "kafka-log-source")
                        .map(JSON::parseObject)
                        .uid("log_parse");
        //logDs.print();

        // ------------------------------------------------------------------------------------------
        // 3) Kafka 用户维度流（kafkaUserDs）
        // ------------------------------------------------------------------------------------------
        KafkaSource<String> kafkaUserSource = KafkaUtils.buildKafkaSource(
                KAFKA_SERVERS,
                TOPIC_USER,
                "flink_join_group",
                OffsetsInitializer.earliest()
        );

        DataStream<JSONObject> userDs =
                env.fromSource(
                                kafkaUserSource,
                                WaterMarkUtils.publicAssignWatermarkStrategyUseGsonParse("ts", 5),
                                "kafka-user-source"
                        )
                        .map(JSON::parseObject)
                        .map(json -> json.getJSONObject("after"))   // ⭐ 只要 after
                        .filter(after -> after != null && after.getString("user_id") != null) // ⭐ 清洗掉空值
                        .name("user_after_extract");



        //userDs.print();

        // ------------------------------------------------------------------------------------------
        // 4) 用户维度广播（最正确方式）
        // ------------------------------------------------------------------------------------------
        MapStateDescriptor<String, JSONObject> userStateDesc =
                new MapStateDescriptor<>(
                        "userInfoBroadcastState",
                        TypeInformation.of(String.class),
                        TypeInformation.of(JSONObject.class)
                );

        BroadcastStream<JSONObject> userBroadcastStream = userDs.broadcast(userStateDesc);

        // ------------------------------------------------------------------------------------------
        // 5) 第一阶段 join：日志流 logDs JOIN 用户维度 userDs（广播）
        // ------------------------------------------------------------------------------------------
        SingleOutputStreamOperator<JSONObject> logJoinUser =
                logDs.connect(userBroadcastStream)
                        .process(new BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>() {

                            @Override
                            public void processElement(JSONObject log,
                                                       ReadOnlyContext readOnlyContext,
                                                       Collector<JSONObject> out) throws Exception {
                                ReadOnlyBroadcastState<String, JSONObject> state =
                                        readOnlyContext.getBroadcastState(userStateDesc);

                                JSONObject user = state.get(log.getString("user_id"));
                                if (user != null) {
                                    log.putAll(user);   // 自动包含 keywords
                                }
                                out.collect(log);
                            }

                            @Override
                            public void processBroadcastElement(JSONObject user,
                                                                Context ctx,
                                                                Collector<JSONObject> out) throws Exception {
                                BroadcastState<String, JSONObject> state =
                                        ctx.getBroadcastState(userStateDesc);
                                state.put(user.getString("user_id"), user);
                            }
                        });

        // ------------------------------------------------------------------------------------------
        // 6) 第二阶段 join：commentDs JOIN (log+user) —— 真正双流 join
        // ------------------------------------------------------------------------------------------
        SingleOutputStreamOperator<JSONObject> finalJoin =
                commentDs
                        .connect(logJoinUser)
                        .keyBy(
                                c -> c.getString("user_id"),
                                l -> l.getString("user_id")
                        )
                        .process(new CoProcessFunction<JSONObject, JSONObject, JSONObject>() {

                            private ValueState<JSONObject> commentState;
                            private ValueState<JSONObject> logState;

                            @Override
                            public void open(Configuration parameters) {
                                commentState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("commentState", JSONObject.class));
                                logState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("logState", JSONObject.class));
                            }

                            @Override
                            public void processElement1(JSONObject comment,
                                                        Context ctx,
                                                        Collector<JSONObject> out) throws Exception {

                                JSONObject log = logState.value();
                                if (log != null) {
                                    comment.putAll(log);
                                    out.collect(comment);
                                }
                                commentState.update(comment);
                            }

                            @Override
                            public void processElement2(JSONObject log,
                                                        Context ctx,
                                                        Collector<JSONObject> out) throws Exception {

                                JSONObject comment = commentState.value();
                                if (comment != null) {
                                    comment.putAll(log);
                                    out.collect(comment);
                                }
                                logState.update(log);
                            }
                        });

        // ------------------------------------------------------------------------------------------
        // 7) 消费水平计算（原有逻辑）
        // ------------------------------------------------------------------------------------------
        DataStream<JSONObject> lastResult =
                finalJoin.keyBy(o -> o.getString("user_id"))
                        .process(new ConsumptionLevelProcessFunction());

        // 输出到控制台
//        lastResult.print("FINAL_JOIN");

        DataStream<JSONObject> withTags = lastResult.map(new UserTagFunction());

//        withTags.print("FINAL_RESULT");

        DataStream<JSONObject> finalFormatted =
                withTags.map(new UserFormatFunction());

        //finalFormatted.print("FINAL_OUTPUT");

        // 8) 将 finalFormatted 输出到 Kafka（Doris Routine Load 消费）
        DataStream<JSONObject> flattenedUser = finalFormatted
                .keyBy(json -> json.getString("userid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> latestState;

                    @Override
                    public void open(Configuration parameters) {
                        latestState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("latestUser", JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject current = latestState.value();

                        if (current == null ||
                                (value.getString("ts") != null
                                        && value.getString("ts").compareTo(current.getString("ts")) > 0)) {

                            latestState.update(value);
                            out.collect(value);   // ⭐ 关键：输出最新值
                        }
                    }
                });

        flattenedUser.print();

// 将 ValueState 输出为扁平化 JSON 字符串
        DataStream<String> kafkaFormatted = flattenedUser.map(json -> {
            JSONObject out = new JSONObject();

            // 基础字段
            out.put("userid", json.getString("userid") != null ? json.getString("userid") : "");
            out.put("username", json.getString("username") != null ? json.getString("username") : "");

            // user_base_info
            JSONObject userBase = json.getJSONObject("user_base_info");
            if (userBase == null) userBase = new JSONObject();
            out.put("birthday", userBase.getString("birthday") != null ? userBase.getString("birthday") : "");
            out.put("decade", userBase.getString("decade") != null ? userBase.getString("decade") : "");
            out.put("gender", userBase.getString("gender") != null ? userBase.getString("gender") : "");
            out.put("zodiac_sign", userBase.getString("zodiac_sign") != null ? userBase.getString("zodiac_sign") : "");
            out.put("weight", userBase.getString("weight") != null ? userBase.getString("weight") : "");
            out.put("age", userBase.getInteger("age") != null ? userBase.getInteger("age") : 0);
            out.put("age_group", userBase.getString("age_group") != null ? userBase.getString("age_group") : "");

            // login_time 数组转 JSON 字符串
            out.put("login_time", json.getJSONArray("login_time") != null ? json.getJSONArray("login_time").toJSONString() : "[]");

            // 消费等级
            out.put("consumption_level", json.getString("consumption_level") != null ? json.getString("consumption_level") : "");

            // device_info
            JSONObject deviceInfo = json.getJSONObject("device_info");
            if (deviceInfo == null) deviceInfo = new JSONObject();
            out.put("device_brand", deviceInfo.getString("brand") != null ? deviceInfo.getString("brand") : "");
            out.put("device_platform", deviceInfo.getString("plat") != null ? deviceInfo.getString("plat") : "");
            out.put("device_platform_version", deviceInfo.getString("platv") != null ? deviceInfo.getString("platv") : "");
            out.put("soft_version", deviceInfo.getString("softv") != null ? deviceInfo.getString("softv") : "");
            out.put("uname", deviceInfo.getString("uname") != null ? deviceInfo.getString("uname") : "");
            out.put("device_name", deviceInfo.getString("device") != null ? deviceInfo.getString("device") : "");

            // search_info
            JSONObject searchInfo = json.getJSONObject("search_info");
            if (searchInfo == null) searchInfo = new JSONObject();
            out.put("search_log_type", searchInfo.getString("log_type") != null ? searchInfo.getString("log_type") : "");

            // category_info
            JSONObject categoryInfo = json.getJSONObject("category_info");
            if (categoryInfo == null) categoryInfo = new JSONObject();
            out.put("category_name", categoryInfo.getString("category_name") != null ? categoryInfo.getString("category_name") : "");
            out.put("product_name", categoryInfo.getString("product_name") != null ? categoryInfo.getString("product_name") : "");
            out.put("goods_name", categoryInfo.getString("goods_name") != null ? categoryInfo.getString("goods_name") : "");

            // shopping_gender
            JSONObject shoppingGender = json.getJSONObject("shopping_gender");
            if (shoppingGender == null) shoppingGender = new JSONObject();
            out.put("shopping_gender", shoppingGender.getString("gender") != null ? shoppingGender.getString("gender") : "");

            // 敏感词及检查标识
            out.put("is_check_sensitive_comment", json.getString("is_check_sensitive_comment") != null ? json.getString("is_check_sensitive_comment") : "0");
            out.put("sensitive_word", json.getJSONArray("sensitive_word") != null ? json.getJSONArray("sensitive_word").toJSONString() : "[]");

            // 时间字段
            out.put("ds", json.getString("ds") != null ? json.getString("ds") : "");
            out.put("ts", json.getString("ts") != null ? json.getString("ts") : "");

            return out.toJSONString();
        });

// 发送到 Kafka（Doris Routine Load 消费）
        kafkaFormatted.print();
        kafkaFormatted.sinkTo(KafkaUtils.buildKafkaSinkOrigin(KAFKA_SERVERS, ORDER_COMMENT_OUTPUT_TOPIC));




        env.execute("Flink-Three-Stream-Join");
    }
}
