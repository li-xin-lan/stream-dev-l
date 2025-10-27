package lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.KafkaUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
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
 * @Author: lxl
 * @Date: 2025/10/26 🐕 🎇
 */
public class DbusSyncSqlserverOmsSysData2Kafka {
    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_order_info";
    private static final String KAFKA_BOTSTRAP_SERVERS = ("192.168.142.30:9092,192.168.142.31:9092,192.168.142.32:9092");

    @SneakyThrows
    public static void main(String[] args) {

        boolean kafkaTopicDelFlag = KafkaUtils.kafkaTopicExists(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC);

        KafkaUtils.createKafkaTopic(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC, 3, (short) 1, kafkaTopicDelFlag);
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("192.168.142.32")
                .port(1433)
                .username("sa")
                .password("Lxl200538,./")
                .database("realtime_v3")
                .tableList("dbo.oms_order_dtl")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");

        SingleOutputStreamOperator<JSONObject> convertStr2JsonDS = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDS")
                .name("convertStr2JsonDS");

        SingleOutputStreamOperator<JSONObject> map = convertStr2JsonDS.map(new MapMergeJsonData());

        map.print("Sink To Kafka Data: ->");


        map.map(json -> json.toJSONString())  // 使用 Lambda 表达式明确调用
                .sinkTo(KafkaUtils.buildKafkaSinkOrigin(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC));

        env.execute("DbusSyncSqlserverOmsSysData2Kafka");
    }
}