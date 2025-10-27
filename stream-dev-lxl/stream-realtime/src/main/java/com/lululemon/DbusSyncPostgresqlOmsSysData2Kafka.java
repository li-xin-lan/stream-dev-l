package lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: lxl
 * @Date: 2025/10/26 🐕 🎇
 */
public class DbusSyncPostgresqlOmsSysData2Kafka {

    @SneakyThrows
    public static void main(String[] args) {

        /*boolean kafkaTopicDelFlag = KafkaUtils.kafkaTopicExists(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);

        KafkaUtils.createKafkaTopic(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC,3,(short) 1,kafkaTopicDelFlag);*/
        System.setProperty("HADOOP_USER_NAME","root");


        DebeziumDeserializationSchema<String> deserializer = new JsonDebeziumDeserializationSchema();
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("cdh02")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.spider_lululemon_jd_product_dtl")
                        .username("postgres")
                        .password("lxl200538")
                        .slotName("flink_etl_cdc_test")
                        .deserializer(deserializer)
                        .decodingPluginName("pgoutput")
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> dataStreamSource = env.fromSource(
                postgresIncrementalSource,
                WatermarkStrategy.noWatermarks(),
                "PostgresParallelSource");
        //.setParallelism(2);
                //.print();
        SingleOutputStreamOperator<JSONObject> convertStr2JsonDS = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDS")
                .name("convertStr2JsonDS");

//        convertStr2JsonDS.print("convertStr2JsonDS -> ");

        convertStr2JsonDS.map(new MapMergeJsonData()).print();


        /*convertStr2JsonDS.print("Sink To Kafka Data: ->");
        convertStr2JsonDS.sinkTo(
                KafkaUtils.buildKafkaSinkOrigin(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC)
        );*/






        env.execute();
    }
}
