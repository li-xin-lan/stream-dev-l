package com.stream.realtime.lululemon.practice;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.KafkaUtils;
import com.stream.realtime.lululemon.practice.func.MapImortantJsonData;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @create 2025-10-31-19:15
 */
public class FlinkSourceKafkaLogs {

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_logs_data";
    private static final String KAFKA_BOTSTRAP_SERVERS = ("172.22.78.0:9092");

    @SneakyThrows
    public static void main(String[] args) {

        boolean kafkaTopicExists = KafkaUtils.kafkaTopicExists(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);

        KafkaUtils.createKafkaTopic(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC,3,(short) 1,kafkaTopicExists);

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        String bootstrapServers = "172.22.78.0:9092"; // 替换为实际的 Kafka 地址
        String topic = "realtime_v3_logs"; // 替换为要消费的 topic
        String groupId = "flink-kafka-logs-group"; // 消费组 ID

        // 创建 Kafka Source
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                bootstrapServers,
                topic,
                groupId,
                OffsetsInitializer.earliest() // 从最早开始消费
        );

        // 从 Kafka 读取数据
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        SingleOutputStreamOperator<JSONObject> string2Json = kafkaStream.map(JSON::parseObject)
                .uid("string2Json")
                .name("string2Json");

        SingleOutputStreamOperator<JSONObject> jsonData = string2Json.map(new MapImortantJsonData());

        SingleOutputStreamOperator<String> strData = jsonData.map(JSON -> JSON.toString());
        strData.sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC));

        env.execute("Flink Read Kafka Data");
    }
}
