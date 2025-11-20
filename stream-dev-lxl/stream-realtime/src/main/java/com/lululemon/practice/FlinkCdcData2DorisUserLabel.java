package com.stream.realtime.lululemon.practice;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.KafkaUtils;
import com.stream.realtime.lululemon.practice.func.ConsumptionLevelProcessFunction;
import com.stream.realtime.lululemon.practice.func.MapSqlServerData;
import com.stream.realtime.lululemon.practice.func.ReadHbaseData;
import com.stream.realtime.lululemon.practice.utils.WaterMarkUtils;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
/**
 * @date 2025/11/13 13:24
 * @description FlinkCdcSqlserver2Kafka
 */
public class FlinkCdcData2DorisUserLabel {

    private static final String KAFKA_BOTSTRAP_SERVERS = "172.22.78.0:9092";
    private static final String KAFKA_LOG_DATA_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_USER_INFO_DATA_TOPIC = "realtime_v3_dim_user_info_v3";


    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        // 1. SQL Server CDC Source
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("localhost")
                .port(1433)
                .username("SA")
                .password("Lxl200538,./")
                .database("realtime_v3")
                .tableList("dbo.order_comment")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> order_comment_data = env.addSource(sqlServerSource, "_order_comment_log");

        SingleOutputStreamOperator<JSONObject> commentStr2JsonDS = order_comment_data.map(JSON::parseObject)
                .uid("commentStr2JsonDS")
                .name("commentStr2JsonDS");

        DataStream<JSONObject> processedData = commentStr2JsonDS.map(new MapSqlServerData());

        // 按用户ID分组并计算消费水平
        DataStream<JSONObject> result = processedData
                .keyBy(json -> json.getString("user_id"))
                .process(new ConsumptionLevelProcessFunction());

        result.print();



        // KAFKA logs 数据
        KafkaSource<String> kafkaLogSource = KafkaUtils.buildKafkaSource(
                KAFKA_BOTSTRAP_SERVERS,
                KAFKA_LOG_DATA_TOPIC,
                "flink_join_group",
                OffsetsInitializer.earliest()
        );

        DataStreamSource<String> kafkaLogDs = env.fromSource(
                kafkaLogSource,
                WaterMarkUtils.publicAssignWatermarkStrategyUseGsonParse("ts", 5),
                "kafka_log_source"
        );

//        kafkaLogDs.print();


        // KAFKA dim-user-info 数据
        KafkaSource<String> kafkaUserSource = KafkaUtils.buildKafkaSource(
                KAFKA_BOTSTRAP_SERVERS,
                KAFKA_USER_INFO_DATA_TOPIC,
                "flink_join_group",
                OffsetsInitializer.earliest()
        );

        DataStreamSource<String> kafkaUserDs = env.fromSource(
                kafkaUserSource,
                WaterMarkUtils.publicAssignWatermarkStrategyUseGsonParse("ts", 5),
                "kafka_user_source"
        );

//        kafkaUserDs.print();





        env.execute("test");
    }

}