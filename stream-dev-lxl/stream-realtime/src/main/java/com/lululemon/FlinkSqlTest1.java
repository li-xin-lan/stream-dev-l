package lululemon;

import com.stream.core.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class FlinkSqlTest1 {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 先调用默认参数设置
        EnvironmentSettingUtils.defaultParameter(env);

        // === 关键：在 defaultParameter 之后重新配置状态后端 ===
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/sql_idea/workspace/stream-dev-realtime/checkpoints");
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(1200000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        // === 在 TableEnvironment 中也配置 ===
        tenv.getConfig().getConfiguration().setString("state.backend", "hashmap");
        tenv.getConfig().getConfiguration().setString("execution.checkpointing.storage", "filesystem");
        tenv.getConfig().getConfiguration().setString("execution.checkpointing.path", "file:///D:/sql_idea/workspace/stream-dev-realtime/checkpoints");


        // 先删除表（如果存在），然后重新创建
        try {
            tenv.executeSql("DROP TABLE IF EXISTS t_kafka_oms_order_info");
            tenv.executeSql("DROP TABLE IF EXISTS sales_amount_result");
        } catch (Exception e) {
            // 忽略删除表的异常
        }

        String source_kafka_order_info_ddl = "create table if not exists t_kafka_oms_order_info (\n" +
                "    id string,\n" +
                "    order_id string,\n" +
                "    user_id string,\n" +
                "    user_name string,\n" +
                "    phone_number string,\n" +
                "    product_link string,\n" +
                "    product_id string,\n" +
                "    color string,\n" +
                "    size string,\n" +
                "    item_id string,\n" +
                "    material string,\n" +
                "    sale_num string,\n" +
                "    sale_amount string,\n" +
                "    total_amount string,\n" +
                "    product_name string,\n" +
                "    is_online_sales string,\n" +
                "    shipping_address string,\n" +
                "    recommendations_product_ids string,\n" +
                "    ds string,\n" +
                "    ts bigint,\n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end,\n" +
                "    insert_time string,\n" +

                "    op string,\n" +
                "    watermark for ts_ms as ts_ms - interval '5' second\n" +
                ")\n" +
                "with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_order_info',\n" +
                "    'properties.bootstrap.servers'= 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";

        tenv.executeSql(source_kafka_order_info_ddl);
        System.out.println("源表创建成功");

        // 验证表是否存在
        //System.out.println("=== 验证表 ===");
        //tenv.executeSql("SHOW TABLES").print();

        /*tenv.executeSql("SELECT id,total_amount,ts_ms FROM t_kafka_oms_order_info WHERE " +
                "CAST(ts_ms AS TIMESTAMP(3)) >= TIMESTAMP '2025-10-27 00:00:00' " +
                "AND CAST(ts_ms AS TIMESTAMP(3)) < TIMESTAMP '2025-10-28 00:00:00' " +
                "LIMIT 10").print();*/


        /*tenv.executeSql("SELECT \n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    CAST(SUM(TRY_CAST(total_amount AS DECIMAL(10, 2))) OVER (ORDER BY window_start) AS DECIMAL(10, 2)) as sale_total_amount\n" +
                "FROM (\n" +
                "    SELECT \n" +
                "        TUMBLE_START(ts_ms, INTERVAL '10' MINUTE) as window_start,\n" +
                "        TUMBLE_END(ts_ms, INTERVAL '10' MINUTE) as window_end,\n" +
                "        total_amount\n" +
                "    FROM t_kafka_oms_order_info\n" +
                "    WHERE CAST(ts_ms AS TIMESTAMP(3)) >= TIMESTAMP '2025-10-27 00:00:00'\n" +
                "      AND CAST(ts_ms AS TIMESTAMP(3)) < TIMESTAMP '2025-10-28 00:00:00'\n" +
                "      AND total_amount IS NOT NULL \n" +
                "      AND total_amount <> ''\n" +
                ")").print();*/

        tenv.executeSql("SELECT \n" +
                "    window_start, \n" +
                "    window_end, \n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as total_sum\n" +
                "  FROM TABLE(\n" +
                "    HOP(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' MINUTES, INTERVAL '10' MINUTES)\n" +
                ")\n" +
                "WHERE DATE_FORMAT(ts_ms, 'yyyy-MM-dd') = '2025-10-27'\n" +
                "GROUP BY window_start, window_end").print();



        env.execute();
    }
}