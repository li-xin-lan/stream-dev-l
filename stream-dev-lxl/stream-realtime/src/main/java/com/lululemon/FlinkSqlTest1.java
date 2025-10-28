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
        tenv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "30s");
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

        String source_kafka_order_info_ddl =
                "CREATE TABLE IF NOT EXISTS t_kafka_oms_order_info (\n" +
                        "    id                         STRING,     \n" +
                        "    order_id                   STRING,     \n" +
                        "    user_id                    STRING,     \n" +
                        "    user_name                  STRING,     \n" +
                        "    phone_number               STRING,     \n" +
                        "    product_link               STRING,     \n" +
                        "    product_id                 STRING,     \n" +
                        "    color                      STRING,     \n" +
                        "    size                       STRING,     \n" +
                        "    item_id                    STRING,     \n" +
                        "    material                   STRING,     \n" +
                        "    sale_num                   STRING,     \n" +
                        "    sale_amount                STRING,     \n" +
                        "    total_amount               STRING,     \n" +
                        "    product_name               STRING,     \n" +
                        "    is_online_sales            STRING,     \n" +
                        "    shipping_address           STRING,     \n" +
                        "    recommendations_product_ids STRING,     \n" +
                        "    ds                         STRING,     \n" +
                        "    ts                         BIGINT,     \n" +
                        "    ts_ms AS CASE                          \n" +
                        "        WHEN ts < 100000000000 THEN to_timestamp_ltz(ts * 1000, 3)        \n" +
                        "        ELSE to_timestamp_ltz(ts, 3)                                      \n" +
                        "    END,                                                                  \n" +
                        "    insert_time                STRING,                                    \n" +
                        "    op                         STRING,                                    \n" +
                        "    WATERMARK FOR ts_ms AS ts_ms - INTERVAL '5' SECOND                    \n" +
                        ")                                                                         \n" +
                        "WITH (                                                                    \n" +
                        "    'connector'                      = 'kafka',                           \n" +
                        "    'topic'                          = 'realtime_v3_order_info',          \n" +
                        "    'properties.bootstrap.servers'   = 'cdh01:9092,cdh02:9092,cdh03:9092',\n" +
                        "    'properties.group.id'            = 'order-analysis1',                 \n" +
                        "    'scan.startup.mode'              = 'earliest-offset',                 \n" +
                        "    'format'                         = 'json',                            \n" +
                        "    'json.fail-on-missing-field'     = 'false',                           \n" +
                        "    'json.ignore-parse-errors'       = 'true'                             \n" +
                        ")";

        tenv.executeSql(source_kafka_order_info_ddl);
        System.out.println("源表创建成功");

        /*tenv.executeSql("SELECT \n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as window_sum\n" +
                "FROM TABLE(\n" +
                "    TUMBLE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' seconds)\n" +
                ")\n" +
                "WHERE CAST(ts_ms AS DATE) = DATE '2025-10-27'\n" +
                "  AND total_amount IS NOT NULL \n" +
                "  AND total_amount <> ''\n" +
                "GROUP BY window_start, window_end").print();*/
       // TODO: 2025/10/28  窗口累计金额

     tenv.executeSql("SELECT \n" +
                "    window_start,\n" +
                "    window_end,\n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as total_cumulative_amount\n" +
                "FROM TABLE(\n" +
                "    CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' seconds, INTERVAL '1' day)\n" +
                ")\n" +
                "WHERE CAST(ts_ms AS DATE) = DATE '2025-10-27'\n" +
                "  AND total_amount IS NOT NULL \n" +
                "  AND total_amount <> ''\n" +
                "GROUP BY window_start, window_end").print();


        // TODO: 2025/10/28 1.窗口累计金额


        tenv.executeSql("WITH CumulativeSales AS (                                                                       \n" +
                "    SELECT                                                                                                \n" +
                "        window_start,                                                                                     \n" +
                "        window_end,                                                                                       \n" +
                "        SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as sale_total_amount                                 \n" +
                "    FROM TABLE(                                                                                           \n" +
                "        CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' seconds, INTERVAL '1' day)\n" +
                "    )                                                                                                     \n" +
                "    WHERE CAST(ts_ms AS DATE) = DATE '2025-10-27'                                                         \n" +
                "      AND total_amount IS NOT NULL                                                                        \n" +
                "      AND total_amount <> ''                                                                              \n" +
                "    GROUP BY window_start, window_end                                                                     \n" +
                ")                                                                                                         \n" +
                "SELECT                                                                                                    \n" +
                "    window_start,                                                                                         \n" +
                "    window_end,                                                                                           \n" +
                "    sale_total_amount                                                                                     \n" +
                "FROM CumulativeSales").print();


        // TODO: 2025/10/28  2.窗口销量top3

        String top5PerWindowSql = "SELECT                                                   \n" +
                "    window_start,                                                          \n" +
                "    window_end,                                                            \n" +
                "    CONCAT(                                                                \n" +
                "        MAX(CASE WHEN rank_num = 1 THEN id ELSE '' END), ',',              \n" +
                "        MAX(CASE WHEN rank_num = 2 THEN id ELSE '' END), ',',              \n" +
                "        MAX(CASE WHEN rank_num = 3 THEN id ELSE '' END), ',',              \n" +
                "        MAX(CASE WHEN rank_num = 4 THEN id ELSE '' END), ',',              \n" +
                "        MAX(CASE WHEN rank_num = 5 THEN id ELSE '' END)                    \n" +
                "    ) as top5_ids                                                          \n" +
                "FROM (                                                                     \n" +
                "    SELECT                                                                 \n" +
                "        window_start,                                                      \n" +
                "        window_end,                                                        \n" +
                "        id,                                                                \n" +
                "        SUM(TRY_CAST(sale_num AS INT)) as total_sale_num,                  \n" +
                "        ROW_NUMBER() OVER (                                                \n" +
                "            PARTITION BY window_start, window_end                          \n" +
                "            ORDER BY SUM(TRY_CAST(sale_num AS INT)) DESC                   \n" +
                "        ) as rank_num                                                      \n" +
                "    FROM TABLE(                                                            \n" +
                "        TUMBLE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' Minutes)\n" +
                "    )                                                                      \n" +
                "    WHERE CAST(ts_ms AS DATE) = DATE '2025-10-27'                          \n" +
                "      AND sale_num IS NOT NULL                                             \n" +
                "      AND sale_num <> ''                                                   \n" +
                "      AND id IS NOT NULL                                                   \n" +
                "      AND id <> ''                                                         \n" +
                "    GROUP BY window_start, window_end, id                                  \n" +
                ")                                                                          \n" +
                "WHERE rank_num <= 5                                                        \n" +
                "GROUP BY window_start, window_end";

        String cumulativeSalesSql = "SELECT                                                 \n" +
                "    window_start,                                                          \n" +
                "    window_end,                                                            \n" +
                "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as total_cumulative_amount\n" +
                "FROM TABLE(                                                                \n" +
                "    CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms),              \n" +
                "    INTERVAL '10' Minutes,                                                 \n" +
                "    INTERVAL '1' day)                                                      \n" +
                ")                                                                          \n" +
                "WHERE CAST(ts_ms AS DATE) = DATE '2025-10-27'                              \n" +
                "  AND total_amount IS NOT NULL                                             \n" +
                "  AND total_amount <> ''                                                   \n" +
                "GROUP BY window_start, window_end";

        tenv.executeSql(top5PerWindowSql).print();
        tenv.executeSql(cumulativeSalesSql).print();




        // TODO: 2025/10/28 3.销售金额累加过程


        String simpleCumulativeSalesSql =
                "SELECT                                                                             \n" +
                        "    window_start,                                                                  \n" +
                        "    window_end,                                                                    \n" +
                        "    SUM(TRY_CAST(total_amount AS DECIMAL(10,2))) as period_amount,                 \n" +
                        "    LISTAGG(                                                                       \n" +
                        "        id || '(' || CAST(TRY_CAST(total_amount AS DECIMAL(10,2)) AS VARCHAR) || ')', \n" +
                        "        '+'                                                                        \n" +
                        "    ) as cumulative_process                                                        \n" +
                        "FROM TABLE(                                                                        \n" +
                        "    CUMULATE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms),                      \n" +
                        "        INTERVAL '10' MINUTES,                                                     \n" +
                        "        INTERVAL '1' DAY)                                                          \n" +
                        ")                                                                                  \n" +
                        "WHERE CAST(ts_ms AS DATE) = DATE '2025-10-27'                                      \n" +
                        "  AND total_amount IS NOT NULL                                                     \n" +
                        "  AND total_amount <> ''                                                           \n" +
                        "GROUP BY window_start, window_end";
        tenv.executeSql(simpleCumulativeSalesSql).print();

        env.execute();
    }
}