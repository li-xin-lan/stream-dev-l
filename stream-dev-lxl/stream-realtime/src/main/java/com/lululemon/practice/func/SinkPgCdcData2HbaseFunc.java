package com.stream.realtime.lululemon.practice.func;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.HbaseUtils;
import com.stream.core.utils.KafkaUtils;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonParser;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.Map;


public class SinkPgCdcData2HbaseFunc extends RichSinkFunction<JsonObject>  implements CheckpointedFunction {

    private static final Logger logger = LoggerFactory.getLogger(SinkPgCdcData2HbaseFunc.class);

    private HbaseUtils hbaseUtils;
    private Connection hbaseConn;
    private String pgHbaseUserInfoTableName = "realtime_v3:dim_user_info_v3";
    private BufferedMutatorParams bufferedMutator;

    BufferedMutator Mutator = null;

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_dim_user_info_v3";
    private static final String KAFKA_BOTSTRAP_SERVERS = ("172.31.123.121:9092");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils("cdh01,cdh02,cdh03");
        hbaseConn = hbaseUtils.getConnection();
        if (!hbaseUtils.tableIsExists(pgHbaseUserInfoTableName)){
            hbaseUtils.createTable("realtime_v3","dim_user_info_v3");
        }

        bufferedMutator = new BufferedMutatorParams(hbaseConn.getTable(TableName.valueOf(pgHbaseUserInfoTableName)).getName()).writeBufferSize(1024);
        Mutator = hbaseConn.getBufferedMutator(bufferedMutator);
    }


    @Override
    public void invoke(JsonObject value, Context context) throws Exception {
        String userIdRowKey = MD5Hash.getMD5AsHex(value.get("user_id").getAsString().getBytes());
        Put put = new Put(Bytes.toBytes(userIdRowKey));
        for (Map.Entry<String, JsonElement> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue().toString()));
        }

        try {
            Mutator.mutate(put);
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    @Override
    public void close() throws Exception {
        super.close();
        if (hbaseConn != null){
            hbaseConn.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (Mutator != null){
            Mutator.close();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        // 2. PostgreSQL CDC Source
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("172.31.123.121")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.user_info_base")
                        .username("postgres")
                        .password("Csq0573,./")
                        .slotName("slot_read_pg_cdc_data_source_flk")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .decodingPluginName("pgoutput")
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .build();

        DataStreamSource<String> pgCdcDs = env.fromSource(postgresIncrementalSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)), "_transaction_pg_cdc");

        SingleOutputStreamOperator<JsonObject> convertPgCdc2JsonDs = pgCdcDs.map(data -> JsonParser.parseString(data).getAsJsonObject())
                .uid("_convert_pgCdc2json")
                .name("convertPgCdc2json");

        SingleOutputStreamOperator<JsonObject> pGdataDs = convertPgCdc2JsonDs.map(new MapMergeJsonData())
                .uid("_MapPgCdcJsonData")
                .name("MapPgCdcJsonData");

        SingleOutputStreamOperator<JsonObject> resultPgCdcDs = pGdataDs.map(data -> {
                    data.remove("ts");
                    return data;
                })
                .uid("_MapRemoveTs")
                .name("MapRemoveTs");

//        resultPgCdcDs.addSink(new SinkPgCdcData2HbaseFunc());


        resultPgCdcDs.map(json -> json.toString())
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOTSTRAP_SERVERS,OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC));


        env.execute("pg2hbase");
    }


}
