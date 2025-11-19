package lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.realtime.lululemon.func.SinkPgCdcData2HbaseFunc;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class DbusSynSqlserverComment {

    private static final String KAFKA_SERVERS = "172.22.78.0:9092";
    private static final String ORDER_COMMENT_TOPIC = "realtime_v3_order_comment_info";
    private static final String LOGS_TOPIC = "realtime_v3_logs";

    // ----------- 敏感词 P0文件路径 -----------
    private static final Set<String> P0_WORDS = new HashSet<>();
    private static final String P0_WORD_PATH = "D:\\sql_idea\\workspace\\stream-dev-realtime\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\suspected-sensitive-words.txt"; // 攻击性词文件

    // ----------- 攻击性言论 P1 文件路径 -----------
    private static final String P1_WORD_PATH = "D:\\sql_idea\\workspace\\stream-dev-realtime\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\offensive-words.txt"; // 攻击性词文件
    private static Set<String> P1_WORDS = new HashSet<>();

    // Trie 树节点类
    static class TrieNode {
        Map<Character, TrieNode> children = new HashMap<>();
        boolean isEnd = false;
    }

    private static TrieNode P0_TRIE;
    private static TrieNode P1_TRIE;

    // 加载 P0 敏感词
    private static void loadP0Words() {
        try (BufferedReader br = new BufferedReader(new FileReader(P0_WORD_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    P0_WORDS.add(line);
                }
            }
        } catch (Exception e) {
            System.err.println("❌ P0敏感词文件加载失败: " + P0_WORD_PATH);
            e.printStackTrace();
        }
    }

    // 加载攻击性词文件（每行一个）
    private static void loadP1Words() {
        try (BufferedReader br = new BufferedReader(new FileReader(P1_WORD_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    P1_WORDS.add(line);
                }
            }
        } catch (Exception e) {
            System.err.println("❌ 攻击性词库加载失败: " + P1_WORD_PATH);
            e.printStackTrace();
        }
    }

    // 构建 Trie 树
    private static TrieNode buildTrie(Set<String> words) {
        TrieNode root = new TrieNode();
        for (String word : words) {
            TrieNode node = root;
            for (char c : word.toCharArray()) {
                node = node.children.computeIfAbsent(c, k -> new TrieNode());
            }
            node.isEnd = true;
        }
        return root;
    }

    // 初始化 Trie 树
    private static void initTries() {
        // 加载 P0
        loadP0Words();
        P0_TRIE = buildTrie(P0_WORDS);

        // 加载 P1 攻击性词
        loadP1Words();
        P1_TRIE = buildTrie(P1_WORDS);
    }

    // Trie 树匹配
    private static Set<String> detectByTrie(String text, TrieNode root) {
        Set<String> matched = new HashSet<>();
        char[] chars = text.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            TrieNode node = root;
            int j = i;
            StringBuilder sb = new StringBuilder();
            while (j < chars.length && node.children.containsKey(chars[j])) {
                sb.append(chars[j]);
                node = node.children.get(chars[j]);
                if (node.isEnd) {
                    matched.add(sb.toString());
                }
                j++;
            }
        }
        return matched;
    }

    // 敏感词检测（p0/p1/p2）
    private static Map<String, Object> detectSensitive(String comment) {
        Map<String, Object> res = new HashMap<>();
        if (comment == null || comment.trim().isEmpty()) {
            res.put("level", "p2");
            res.put("is_black", 0);
            res.put("sensitive_words", "");
            return res;
        }

        Set<String> matchedP0 = detectByTrie(comment, P0_TRIE);
        Set<String> matchedP1 = detectByTrie(comment, P1_TRIE);

        Set<String> allMatched = new HashSet<>();
        allMatched.addAll(matchedP0);
        allMatched.addAll(matchedP1);

        String level;
        int isBlack;

        if (!matchedP0.isEmpty()) {
            level = "p0";
            isBlack = 1;
        } else if (!matchedP1.isEmpty()) {
            level = "p1";
            isBlack = 1;
        } else {
            level = "p2";
            isBlack = 0;
        }


        res.put("level", level);
        res.put("is_black", isBlack);
        res.put("sensitive_words", String.join(",", allMatched));

        return res;
    }

    @SneakyThrows
    public static void main(String[] args) {

        // 初始化 Trie 树
        initTries();

        /*// 创建 Kafka topic
        boolean orderCommentTopicExists = KafkaUtils.kafkaTopicExists(KAFKA_SERVERS, ORDER_COMMENT_TOPIC);
        KafkaUtils.createKafkaTopic(KAFKA_SERVERS, ORDER_COMMENT_TOPIC, 3, (short) 1, orderCommentTopicExists);

        boolean logsTopicExists = KafkaUtils.kafkaTopicExists(KAFKA_SERVERS, LOGS_TOPIC);
        KafkaUtils.createKafkaTopic(KAFKA_SERVERS, LOGS_TOPIC, 3, (short) 1, logsTopicExists);*/

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        Properties debeziumProps = new Properties();
        debeziumProps.put("snapshot.mode", "initial");
        debeziumProps.put("snapshot.locking.mode", "none");
        debeziumProps.put("decimal.handling.mode", "string");

        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("localhost")
                .port(1433)
                .username("SA")
                .password("Lxl200538,./")
                .database("realtime_v3")
                .tableList("dbo.order_comment")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProps)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> cdcStream = env.addSource(sqlServerSource, "sqlserver_comment_source");

        // 处理 JSON + 敏感词检测
        SingleOutputStreamOperator<String> resultStream = cdcStream
                .map(jsonStr -> {
                    JSONObject json = JSON.parseObject(jsonStr);
                    JSONObject after = json.getJSONObject("after");
                    if (after == null) return null;

                    String comment = after.getString("comment");

                    // 调用敏感词检测
                    Map<String, Object> r = detectSensitive(comment);

                    JSONObject out = new JSONObject();
                    out.put("user_id", after.getString("user_id"));
                    out.put("order_id", after.getString("order_id"));
                    out.put("product_id", after.getString("product_id"));
                    out.put("comment", comment);
                    out.put("ts", System.currentTimeMillis());

                    // 新增字段
                    out.put("level", r.get("level"));
                    out.put("is_black", r.get("is_black"));
                    out.put("sensitive_words", r.get("sensitive_words"));

                    return out.toJSONString();
                })
                .filter(Objects::nonNull);

        // 输出到控制台
        //resultStream.print().name("console_sink");

        // PostgreSQL 用户信息 CDC 配置
        DebeziumDeserializationSchema<String> deserializer = new JsonDebeziumDeserializationSchema();
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("127.0.0.1")
                        .port(5432)
                        .database("spider_db")
                        .schemaList("public")
                        .tableList("public.user_info_base")
                        .username("postgres")
                        .password("lxl200538")
                        .slotName("flink_etl_cdc_test_" + System.currentTimeMillis()) // 使用时间戳避免冲突
                        .deserializer(deserializer)
                        .decodingPluginName("pgoutput")
                        .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .build();

        DataStreamSource<String> postgresDataStream = env.fromSource(
                postgresIncrementalSource,
                WatermarkStrategy.noWatermarks(),
                "PostgresParallelSource");

        SingleOutputStreamOperator<JSONObject> convertStr2JsonDS = postgresDataStream.map(JSON::parseObject)
                .uid("convertStr2JsonDS")
                .name("convertStr2JsonDS");

        SingleOutputStreamOperator<com.google.gson.JsonObject> hbaseStream = convertStr2JsonDS
                .map(alibabaJson -> {
                    // 将 Alibaba FastJSON 转换为 Gson JsonObject
                    String jsonString = alibabaJson.toJSONString();
                    com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                    return parser.parse(jsonString).getAsJsonObject();
                })
                .returns(com.google.gson.JsonObject.class)
                .uid("convert_to_gson")
                .name("convert_to_gson");

        // 添加到 HBase Sink
        hbaseStream.addSink(new SinkPgCdcData2HbaseFunc())
                .name("hbase_user_info_sink")
                .uid("hbase_user_info_sink");

        // Kafka Logs 数据流配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_SERVERS);
        kafkaProps.setProperty("group.id", "flink-kafka-logs-consumer-group");
        kafkaProps.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> kafkaLogsConsumer = new FlinkKafkaConsumer<>(
                LOGS_TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStreamSource<String> kafkaLogsStream = env.addSource(kafkaLogsConsumer, "kafka_logs_source");

        // 测试输出 Kafka 日志流
        kafkaLogsStream.print("kafka_logs_output");

        // 测试输出用户信息流
        SingleOutputStreamOperator<String> map = convertStr2JsonDS.map(obj -> obj.toJSONString());
        map.print();
        // 测试输出订单评论流
        resultStream.print("order_comment_output");

        System.out.println("🚀 开始执行Flink作业...");
        env.execute("DbusSynSqlserverCommentETL");
    }
}