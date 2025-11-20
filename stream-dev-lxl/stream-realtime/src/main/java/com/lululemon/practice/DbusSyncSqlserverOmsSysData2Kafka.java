package com.stream.realtime.lululemon.practice;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.KafkaUtils;
import com.stream.realtime.lululemon.practice.func.MapMergeJsonDataFunc;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @create 2025-10-25-10:44
 */
public class DbusSyncSqlserverOmsSysData2Kafka {

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_order_info";
    private static final String KAFKA_BOTSTRAP_SERVERS = ("172.22.78.0:9092");

    @SneakyThrows
    public static void main(String[] args) {

        boolean kafkaTopicExists = KafkaUtils.kafkaTopicExists(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);

        KafkaUtils.createKafkaTopic(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC,3,(short) 1,kafkaTopicExists);

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

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


        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
//        dataStreamSource.print().setParallelism(1);

        SingleOutputStreamOperator<JSONObject> convertStr2JsonDS = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDS")
                .name("convertStr2JsonDS");

        convertStr2JsonDS.print("convertStr2JsonDS -> ");

        SingleOutputStreamOperator<JSONObject> jsonData = convertStr2JsonDS.map(new MapMergeJsonDataFunc());

        jsonData.map(json -> json.toString())
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC));



        env.execute("DbusSyncSqlserverOmsSysData2Kafka");
    }


}