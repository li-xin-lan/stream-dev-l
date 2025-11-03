package lululemon;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.stream.core.utils.EnvironmentSettingUtils;
import com.stream.core.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.lionsoul.ip2region.xdb.Searcher;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: lxl
 * @Date: 2025/11/1
 * @Description:
 *  指标1：每日页面访问量统计
 *  指标2：历史天 + 当天搜索词TOP10（词云）
 *  指标3：历史天 + 当天登录区域热力（IP转省份）
 */
public class HistoryTodayPv {
    private static final Gson gson = new Gson();
    // 1️⃣ POJO：原始日志
    public static class UserLog {
        public String user_id;
        public String log_type;
        public String formatted_time;
    }
    // 2️⃣ POJO：聚合结果
    public static class UserProfile {
        public String user_id;
        public Set<String> login_days;
        public boolean has_purchase;
        public boolean has_search;
        public boolean has_view;
        public Set<String> login_periods;
        public long update_time;

        public UserProfile() {}
    // 2️⃣ POJO：聚合结果
    /*public static class UserProfile {
        public String user_id;
        public Set<String> login_days;
        public boolean has_purchase;
        public boolean has_search;
        public boolean has_view;
        public Set<String> login_periods;

        public UserProfile() {}*/

        public UserProfile(String userId, Set<String> days, boolean hasPurchase, boolean hasSearch, boolean hasView, Set<String> periods) {
            this.user_id = userId;
            this.login_days = days;
            this.has_purchase = hasPurchase;
            this.has_search = hasSearch;
            this.has_view = hasView;
            this.login_periods = periods;
        }
        // 转换为JSON字符串，用于写入ES
        public String toJsonString() {
            JSONObject json = new JSONObject();
            json.put("user_id", user_id);
            json.put("login_days", new ArrayList<>(login_days));
            json.put("has_purchase", has_purchase);
            json.put("has_search", has_search);
            json.put("has_view", has_view);
            json.put("login_periods", new ArrayList<>(login_periods));
            json.put("update_time", update_time);
            json.put("login_days_count", login_days.size());
            json.put("login_periods_count", login_periods.size());
            return json.toJSONString();
        }
    }

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("HH");
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ==================== 添加内存配置 ====================
        // 设置较低的并行度
        env.setParallelism(1);  // 先设置为1测试
        // 禁用链式操作，减少缓冲区需求
        env.disableOperatorChaining();
        // 设置缓冲区超时时间
        env.setBufferTimeout(100);  // 100ms
        // 使用配置对象设置内存参数
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        // 设置网络缓冲区数量（直接设置数量而不是大小）
        config.setInteger("taskmanager.network.memory.buffers-per-channel", 2);
        config.setInteger("taskmanager.network.memory.floating-buffers-per-gate", 8);
        // 设置内存大小
        config.setString("taskmanager.memory.network.min", "256mb");
        config.setString("taskmanager.memory.network.max", "512mb");
        config.setString("taskmanager.memory.managed.size", "512mb");
        env.configure(config);
        EnvironmentSettingUtils.defaultParameter(env);
        String bootstrapServers = "192.168.142.32:9092"; // 替换为实际的 Kafka 地址
        String topic = "realtime_v3_logs_data"; // 替换为要消费的 topic
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

        ObjectMapper mapper = new ObjectMapper();
        //kafkaStream.print();
        // ==================== 1️⃣ 解析 JSON ====================
        DataStream<JSONObject> jsonStream = kafkaStream
                .flatMap((FlatMapFunction<String, JSONObject>) (value, out) -> {
                    try {
                        JSONObject json = JSON.parseObject(value);
                        out.collect(json);
                    } catch (Exception ignore) {}
                })
                .returns(TypeInformation.of(JSONObject.class))
                .name("ParseJson");
        //jsonStream.print();


// TODO: 2025/11/3 需求1 页面访问量

        /*DataStream<Tuple3<String, String, Long>> datePageStream = jsonStream
                .flatMap((FlatMapFunction<JSONObject, Tuple3<String, String, Long>>) (jsonObject, out) -> {
                    String logType = jsonObject.getString("log_type");
                    Long timestamp = jsonObject.getLong("ts");
                    if (logType != null && timestamp != null) {
                        long ts = timestamp < 1000000000000L ? timestamp * 1000 : timestamp;
                        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
                        String date = dateTime.format(DATE_FORMATTER);
                        out.collect(new Tuple3<>(date, logType, 1L));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {}))
                .name("ParseDateAndPage");

        SingleOutputStreamOperator<Tuple3<String, String, Long>> dailyPagePv = datePageStream
                .keyBy(value -> value.f0 + "|" + value.f1)
                .sum(2)
                .name("DailyPagePV");

        dailyPagePv.map((MapFunction<Tuple3<String, String, Long>, String>) value -> {
            String currentTime = LocalDateTime.now().format(TIME_FORMATTER);
            return String.format("[%s] 日统计 | 日期: %s | 页面: %-15s | 访问量: %6d",
                    currentTime, value.f0, value.f1, value.f2);
        }).print("页面访问量");*/


// TODO: 2025/11/3 需求2 搜索词 TOP10


        /*DataStream<Tuple2<String, Long>> keywordStream = kafkaStream
                .flatMap((String json, Collector<Tuple2<String, Long>> out) -> {
                    JsonNode node = mapper.readTree(json);
                    if (node.has("keywords")) {
                        String keywords = node.get("keywords").asText();
                        String[] split = keywords.split("[,，]"); // 支持中英文逗号
                        for (String k : split) {
                            k = k.trim();
                            if (!k.isEmpty()) {
                                out.collect(new Tuple2<>(k, 1L));
                            }
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        //  累加每个搜索词的总次数
        DataStream<Tuple2<String, Long>> countStream = keywordStream
                .keyBy(t -> t.f0)
                .sum(1);


        SingleOutputStreamOperator<String> keyWorksTop10 = countStream
                .key  @Override

                        // 取前 10
                        List<Map.Entry<String, LongBy(t -> 0) // 全局排序
                       public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) {
                        counts.put(value.f0, value.f1);
   .process(new KeyedProcessFunction<Integer, Tuple2<String, Long>, String>() {

                    private final Map<String, Long> counts = new HashMap<>();

                  >> top10 = counts.entrySet()
                                 .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .limit(10)
                                .collect(Collectors.toList());

                        StringBuilder sb = new StringBuilder();
                        sb.append("当前TOP10搜索词:\n");
                        for (Map.Entry<String, Long> e : top10) {
                            sb.append("搜索词: ").append(e.getKey()).append(", 次数: ").append(e.getValue()).append("\n");
                        }

                        out.collect(sb.toString());
                    }
                });
        keyWorksTop10.print();*/



// TODO: 2025/11/3 需求3 登录区域热力（IP转地址）


       /* DataStream<Tuple2<String, Long>> regionStream = kafkaStream
                .flatMap((String json, Collector<Tuple2<String, Long>> out) -> {
                    JsonNode node = mapper.readTree(json);
                    if (node.has("log_type") && "login".equals(node.get("log_type").asText())
                            && node.has("region")) {
                        String region = node.get("region").asText().trim();
                        if (!region.isEmpty()) {
                            out.collect(new Tuple2<>(region, 1L));
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        // 按地区累加
        DataStream<Tuple2<String, Long>> regionCountStream = regionStream
                .keyBy(t -> t.f0)
                .sum(1);

        // 输出全国热力情况（每条数据更新一次）
        SingleOutputStreamOperator<String> process = regionCountStream
                .keyBy(t -> 0) // 全局排序/输出
                .process(new KeyedProcessFunction<Integer, Tuple2<String, Long>, String>() {

                    private final Map<String, Long> counts = new HashMap<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) {
                        counts.put(value.f0, value.f1);

                        // 构建输出，可直接用于热力图
                        StringBuilder sb = new StringBuilder();
                        sb.append("全国登录热力统计:\n");
                        for (Map.Entry<String, Long> e : counts.entrySet()) {
                            sb.append("地区: ").append(e.getKey())
                                    .append(", 访问量: ").append(e.getValue()).append("\n");
                        }

                        out.collect(sb.toString());
                    }
                });

        process.print();*/


// TODO: 2025/11/3 需求4 历史天 + 当天 路径分析

        /*DataStream<Tuple2<String, Long>> userPathStream = kafkaStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String json, Collector<Tuple2<String, Long>> out) throws Exception {
                        try {
                            JsonNode node = mapper.readTree(json);

                            // 提取用户行为信息
                            String userId = node.has("user_id") ? node.get("user_id").asText().trim() : "unknown";
                            String logType = node.has("log_type") ? node.get("log_type").asText().trim() : "";
                            String opa = node.has("opa") ? node.get("opa").asText().trim() : "";
                            String pageInfo = node.has("pageinfo") ? node.get("pageinfo").asText().trim() : "";

                            if (!logType.isEmpty()) {
                                // 统计行为类型分布
                                out.collect(new Tuple2<>("行为类型:" + logType, 1L));

                                // 统计操作类型分布
                                if (!opa.isEmpty()) {
                                    out.collect(new Tuple2<>("操作类型:" + opa, 1L));
                                }

                                // 统计页面访问
                                if (!pageInfo.isEmpty()) {
                                    out.collect(new Tuple2<>("页面访问:" + pageInfo, 1L));
                                }

                                // 用户行为序列（按用户ID）
                                if (!userId.equals("unknown")) {
                                    out.collect(new Tuple2<>("用户行为:" + userId + ":" + logType, 1L));
                                }
                            }

                        } catch (Exception e) {
                            // 忽略解析异常
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .name("ExtractUserPathInfo");

// 按行为信息分组累加
        DataStream<Tuple2<String, Long>> userPathCountStream = userPathStream
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .name("UserPathCount");

// 用户路径分析统计（历史天+当天累计）
        SingleOutputStreamOperator<String> userPathAnalysis = userPathCountStream
                .keyBy(new KeySelector<Tuple2<String, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<String, Long> value) throws Exception {
                        return 0;
                    }
                })
                .process(new KeyedProcessFunction<Integer, Tuple2<String, Long>, String>() {

                    private Map<String, Long> pathStats;
                    private Map<String, List<String>> userBehaviorSequences;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pathStats = new HashMap<>();
                        userBehaviorSequences = new HashMap<>();
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        String key = value.f0;
                        Long count = value.f1;

                        // 更新统计
                        pathStats.put(key, count);

                        // 如果是用户行为，更新用户行为序列
                        if (key.startsWith("用户行为:")) {
                            String[] parts = key.split(":");
                            if (parts.length >= 3) {
                                String userId = parts[1];
                                String behavior = parts[2];

                                if (!userBehaviorSequences.containsKey(userId)) {
                                    userBehaviorSequences.put(userId, new ArrayList<String>());
                                }
                                List<String> sequence = userBehaviorSequences.get(userId);
                                if (sequence.isEmpty() || !sequence.get(sequence.size() - 1).equals(behavior)) {
                                    sequence.add(behavior);
                                }
                            }
                        }

                        // 每处理10条数据生成一次报告，避免输出过于频繁
                        if (pathStats.size() % 10 == 0) {
                            String report = buildUserPathReport();
                            out.collect(report);
                        }
                    }

                    private String buildUserPathReport() {
                        if (pathStats.isEmpty()) {
                            return "暂无用户路径数据";
                        }

                        StringBuilder sb = new StringBuilder();
                        String currentTime = LocalDateTime.now().format(TIME_FORMATTER);

                        sb.append(String.format("[%s] 用户路径分析报告（历史天+当天累计）\n", currentTime));
                        // 分隔线
                        for (int i = 0; i < 70; i++) {
                            sb.append("=");
                        }
                        sb.append("\n");

                        // 行为类型分布
                        appendBehaviorTypeReport(sb);

                        // 操作类型分布
                        appendOperationTypeReport(sb);

                        // 页面访问统计
                        appendPageAccessReport(sb);

                        // 用户行为路径分析
                        appendUserPathAnalysis(sb);

                        // 再次添加分隔线
                        for (int i = 0; i < 70; i++) {
                            sb.append("=");
                        }
                        sb.append("\n");
                        return sb.toString();
                    }

                    private void appendBehaviorTypeReport(StringBuilder sb) {
                        List<Map.Entry<String, Long>> behaviors = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : pathStats.entrySet()) {
                            if (entry.getKey().startsWith("行为类型:")) {
                                behaviors.add(entry);
                            }
                        }

                        // 排序
                        Collections.sort(behaviors, new Comparator<Map.Entry<String, Long>>() {
                            @Override
                            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                                return Long.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        sb.append("🎯 用户行为类型分布:\n");
                        if (behaviors.isEmpty()) {
                            sb.append("  暂无行为数据\n");
                        } else {
                            for (int i = 0; i < Math.min(behaviors.size(), 10); i++) {
                                Map.Entry<String, Long> entry = behaviors.get(i);
                                String behavior = entry.getKey().substring(5); // 去掉"行为类型:"前缀
                                sb.append(String.format("  %-15s : %6d次\n", behavior, entry.getValue()));
                            }
                        }
                        sb.append("\n");
                    }

                    private void appendOperationTypeReport(StringBuilder sb) {
                        List<Map.Entry<String, Long>> operations = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : pathStats.entrySet()) {
                            if (entry.getKey().startsWith("操作类型:")) {
                                operations.add(entry);
                            }
                        }

                        Collections.sort(operations, new Comparator<Map.Entry<String, Long>>() {
                            @Override
                            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                                return Long.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        sb.append("🛠️ 用户操作类型分布:\n");
                        if (operations.isEmpty()) {
                            sb.append("  暂无操作数据\n");
                        } else {
                            for (int i = 0; i < Math.min(operations.size(), 8); i++) {
                                Map.Entry<String, Long> entry = operations.get(i);
                                String operation = entry.getKey().substring(5); // 去掉"操作类型:"前缀
                                sb.append(String.format("  %-15s : %6d次\n", operation, entry.getValue()));
                            }
                        }
                        sb.append("\n");
                    }

                    private void appendPageAccessReport(StringBuilder sb) {
                        List<Map.Entry<String, Long>> pages = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : pathStats.entrySet()) {
                            if (entry.getKey().startsWith("页面访问:")) {
                                pages.add(entry);
                            }
                        }

                        Collections.sort(pages, new Comparator<Map.Entry<String, Long>>() {
                            @Override
                            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                                return Long.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        sb.append("📄 页面访问统计:\n");
                        if (pages.isEmpty()) {
                            sb.append("  暂无页面访问数据\n");
                        } else {
                            for (int i = 0; i < Math.min(pages.size(), 8); i++) {
                                Map.Entry<String, Long> entry = pages.get(i);
                                String page = entry.getKey().substring(5); // 去掉"页面访问:"前缀
                                sb.append(String.format("  %-20s : %6d次\n", page, entry.getValue()));
                            }
                        }
                        sb.append("\n");
                    }

                    private void appendUserPathAnalysis(StringBuilder sb) {
                        sb.append("🔄 用户行为路径分析:\n");
                        sb.append(String.format("  活跃用户数: %d\n", userBehaviorSequences.size()));

                        // 统计常见行为路径
                        Map<String, Integer> pathPatterns = new HashMap<>();
                        for (List<String> sequence : userBehaviorSequences.values()) {
                            if (sequence.size() >= 2) {
                                // 将行为序列转换为路径模式
                                String path = String.join(" → ", sequence);
                                pathPatterns.put(path, pathPatterns.getOrDefault(path, 0) + 1);
                            }
                        }

                        // 排序找出最常见的路径
                        List<Map.Entry<String, Integer>> sortedPaths = new ArrayList<>(pathPatterns.entrySet());
                        Collections.sort(sortedPaths, new Comparator<Map.Entry<String, Integer>>() {
                            @Override
                            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                                return Integer.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        if (sortedPaths.isEmpty()) {
                            sb.append("  暂无完整行为路径数据\n");
                        } else {
                            sb.append("  常见行为路径:\n");
                            for (int i = 0; i < Math.min(sortedPaths.size(), 5); i++) {
                                Map.Entry<String, Integer> entry = sortedPaths.get(i);
                                sb.append(String.format("  %-40s : %3d用户\n", entry.getKey(), entry.getValue()));
                            }
                        }
                        sb.append("\n");
                    }
                })
                .name("UserPathAnalysisReport");

        userPathAnalysis.print("用户路径分析");*/

// TODO: 2025/11/3 需求5  用户设备统计（iOS & Android）


        /*DataStream<Tuple2<String, Long>> devicePlatformStream = kafkaStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String json, Collector<Tuple2<String, Long>> out) throws Exception {
                        try {
                            JsonNode node = mapper.readTree(json);

                            // 提取设备信息
                            String brand = node.has("brand") ? node.get("brand").asText().trim() : "";
                            String device = node.has("device") ? node.get("device").asText().trim() : "";
                            String plat = node.has("plat") ? node.get("plat").asText().trim() : "";
                            String platv = node.has("platv") ? node.get("platv").asText().trim() : "";
                            String softv = node.has("softv") ? node.get("softv").asText().trim() : "";

                            if (brand.isEmpty() && plat.isEmpty()) {
                                return;
                            }

                            // 判断平台类型
                            String platformType = "其他";
                            String platLower = plat.toLowerCase();
                            String brandLower = brand.toLowerCase();
                            if (platLower.contains("iphone") || platLower.contains("ios") ||
                                    brandLower.contains("iphone") || brandLower.contains("apple")) {
                                platformType = "iOS";
                            } else if (platLower.contains("android") || brandLower.contains("huawei") ||
                                    brandLower.contains("xiaomi") || brandLower.contains("oppo") ||
                                    brandLower.contains("vivo") || brandLower.contains("samsung")) {
                                platformType = "Android";
                            }

                            // 统计平台类型
                            out.collect(new Tuple2<>("平台类型:" + platformType, 1L));

                            // 统计品牌（按平台分类）
                            if (!brand.isEmpty()) {
                                out.collect(new Tuple2<>(platformType + ":品牌:" + brand, 1L));
                            }

                            // 统计设备型号（按平台分类）
                            if (!device.isEmpty()) {
                                out.collect(new Tuple2<>(platformType + ":设备:" + device, 1L));
                            }

                            // 统计平台版本（按平台分类）
                            if (!platv.isEmpty()) {
                                out.collect(new Tuple2<>(platformType + ":平台版本:" + platv, 1L));
                            }

                            // 统计软件版本（按平台分类）
                            if (!softv.isEmpty()) {
                                out.collect(new Tuple2<>(platformType + ":软件版本:" + softv, 1L));
                            }

                        } catch (Exception e) {
                            // 忽略解析异常
                        }
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .name("ExtractDevicePlatformInfo");

// 按设备信息分组累加
        DataStream<Tuple2<String, Long>> devicePlatformCountStream = devicePlatformStream
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .name("DevicePlatformCount");

// 全局设备平台统计（历史天+当天累计）
        SingleOutputStreamOperator<String> devicePlatformAnalysis = devicePlatformCountStream
                .keyBy(new KeySelector<Tuple2<String, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<String, Long> value) throws Exception {
                        return 0;
                    }
                })
                .process(new KeyedProcessFunction<Integer, Tuple2<String, Long>, String>() {

                    private Map<String, Long> devicePlatformStats;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        devicePlatformStats = new HashMap<>();
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        // 更新统计
                        devicePlatformStats.put(value.f0, value.f1);

                        // 生成报告
                        String report = buildDevicePlatformReport();
                        out.collect(report);
                    }

                    private String buildDevicePlatformReport() {
                        if (devicePlatformStats.isEmpty()) {
                            return "暂无设备数据";
                        }

                        StringBuilder sb = new StringBuilder();
                        String currentTime = LocalDateTime.now().format(TIME_FORMATTER);

                        sb.append(String.format("[%s] 用户设备平台统计（历史天+当天累计）\n", currentTime));
                        // 使用循环代替String.repeat()
                        for (int i = 0; i < 70; i++) {
                            sb.append("=");
                        }
                        sb.append("\n");

                        // 平台类型分布
                        appendPlatformTypeReport(sb);

                        // iOS 详细统计
                        appendPlatformDetailReport(sb, "iOS");

                        // Android 详细统计
                        appendPlatformDetailReport(sb, "Android");

                        // 再次添加分隔线
                        for (int i = 0; i < 70; i++) {
                            sb.append("=");
                        }
                        sb.append("\n");
                        return sb.toString();
                    }

                    private void appendPlatformTypeReport(StringBuilder sb) {
                        long iosCount = devicePlatformStats.containsKey("平台类型:iOS") ? devicePlatformStats.get("平台类型:iOS") : 0L;
                        long androidCount = devicePlatformStats.containsKey("平台类型:Android") ? devicePlatformStats.get("平台类型:Android") : 0L;
                        long otherCount = devicePlatformStats.containsKey("平台类型:其他") ? devicePlatformStats.get("平台类型:其他") : 0L;

                        long total = iosCount + androidCount + otherCount;

                        if (total == 0) {
                            sb.append("📊 平台类型分布: 暂无数据\n\n");
                            return;
                        }

                        // 计算百分比
                        double iosPercent = total > 0 ? (iosCount * 100.0 / total) : 0;
                        double androidPercent = total > 0 ? (androidCount * 100.0 / total) : 0;
                        double otherPercent = total > 0 ? (otherCount * 100.0 / total) : 0;

                        sb.append("📊 平台类型分布:\n");
                        sb.append(String.format("  iOS     : %6d次 (%.1f%%)\n", iosCount, iosPercent));
                        sb.append(String.format("  Android : %6d次 (%.1f%%)\n", androidCount, androidPercent));
                        sb.append(String.format("  其他    : %6d次 (%.1f%%)\n", otherCount, otherPercent));
                        sb.append("\n");
                    }

                    private void appendPlatformDetailReport(StringBuilder sb, String platform) {
                        // 品牌统计
                        List<Map.Entry<String, Long>> brands = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : devicePlatformStats.entrySet()) {
                            if (entry.getKey().startsWith(platform + ":品牌:")) {
                                brands.add(entry);
                            }
                        }
                        // 排序
                        Collections.sort(brands, new Comparator<Map.Entry<String, Long>>() {
                            @Override
                            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                                return Long.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        // 设备统计
                        List<Map.Entry<String, Long>> devices = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : devicePlatformStats.entrySet()) {
                            if (entry.getKey().startsWith(platform + ":设备:")) {
                                devices.add(entry);
                            }
                        }
                        Collections.sort(devices, new Comparator<Map.Entry<String, Long>>() {
                            @Override
                            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                                return Long.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        // 平台版本统计
                        List<Map.Entry<String, Long>> platformVersions = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : devicePlatformStats.entrySet()) {
                            if (entry.getKey().startsWith(platform + ":平台版本:")) {
                                platformVersions.add(entry);
                            }
                        }
                        Collections.sort(platformVersions, new Comparator<Map.Entry<String, Long>>() {
                            @Override
                            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                                return Long.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        // 软件版本统计
                        List<Map.Entry<String, Long>> softwareVersions = new ArrayList<>();
                        for (Map.Entry<String, Long> entry : devicePlatformStats.entrySet()) {
                            if (entry.getKey().startsWith(platform + ":软件版本:")) {
                                softwareVersions.add(entry);
                            }
                        }
                        Collections.sort(softwareVersions, new Comparator<Map.Entry<String, Long>>() {
                            @Override
                            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                                return Long.compare(o2.getValue(), o1.getValue());
                            }
                        });

                        String platformIcon = platform.equals("iOS") ? "🍎" : "🤖";
                        sb.append(platformIcon).append(" ").append(platform).append(" 设备详情:\n");

                        if (!brands.isEmpty()) {
                            sb.append("  品牌分布:\n");
                            for (int i = 0; i < Math.min(brands.size(), 8); i++) {
                                Map.Entry<String, Long> entry = brands.get(i);
                                String brand = entry.getKey().split(":品牌:")[1];
                                sb.append(String.format("    %-12s : %6d次\n", brand, entry.getValue()));
                            }
                        }

                        if (!devices.isEmpty()) {
                            sb.append("  热门设备:\n");
                            for (int i = 0; i < Math.min(devices.size(), 5); i++) {
                                Map.Entry<String, Long> entry = devices.get(i);
                                String device = entry.getKey().split(":设备:")[1];
                                sb.append(String.format("    %-15s : %6d次\n", device, entry.getValue()));
                            }
                        }

                        if (!platformVersions.isEmpty()) {
                            sb.append("  平台版本:\n");
                            for (int i = 0; i < Math.min(platformVersions.size(), 5); i++) {
                                Map.Entry<String, Long> entry = platformVersions.get(i);
                                String version = entry.getKey().split(":平台版本:")[1];
                                sb.append(String.format("    %-10s : %6d次\n", version, entry.getValue()));
                            }
                        }

                        if (!softwareVersions.isEmpty()) {
                            sb.append("  软件版本:\n");
                            for (int i = 0; i < Math.min(softwareVersions.size(), 5); i++) {
                                Map.Entry<String, Long> entry = softwareVersions.get(i);
                                String version = entry.getKey().split(":软件版本:")[1];
                                sb.append(String.format("    %-10s : %6d次\n", version, entry.getValue()));
                            }
                        }
                        sb.append("\n");
                    }
                })
                .name("DevicePlatformAnalysisReport");

        devicePlatformAnalysis.print("用户设备平台统计");*/



// TODO: 2025/11/3 需求6  6 用户画像

        /*DataStream<String> userProfileStream = jsonStream
                .keyBy((KeySelector<JSONObject, String>) json -> {
                    String userId = json.getString("user_id");
                    return userId != null ? userId : "unknown";
                })
                .process(new KeyedProcessFunction<String, JSONObject, String>() {

                    private transient ValueState<UserProfile> profileState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态描述符
                        ValueStateDescriptor<UserProfile> descriptor =
                                new ValueStateDescriptor<>("userProfile", UserProfile.class);
                        profileState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject json, Context ctx, Collector<String> out) throws Exception {
                        String userId = json.getString("user_id");
                        String logType = json.getString("log_type");
                        Long timestamp = json.getLong("ts");

                        if (userId == null || "unknown".equals(userId) || logType == null || timestamp == null) {
                            return;
                        }

                        // 转换时间戳
                        long ts = timestamp < 1000000000000L ? timestamp * 1000 : timestamp;
                        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());

                        // 获取日期和时段
                        String date = dateTime.format(DATE_FORMATTER);
                        String hourPeriod = getTimePeriod(dateTime);

                        // 获取或创建用户画像
                        UserProfile profile = profileState.value();
                        if (profile == null) {
                            profile = new UserProfile(userId, new HashSet<>(), false, false, false, new HashSet<>());
                        }

                        // 更新登录天数
                        profile.login_days.add(date);

                        // 更新登录时段
                        profile.login_periods.add(hourPeriod);

                        // 更新行为标志
                        switch (logType) {
                            case "purchase":
                                profile.has_purchase = true;
                                break;
                            case "search":
                                profile.has_search = true;
                                break;
                            case "view":
                                profile.has_view = true;
                                break;
                        }

                        // 更新时间戳
                        profile.update_time = System.currentTimeMillis();

                        // 保存状态
                        profileState.update(profile);

                        // 输出更新后的用户画像
                        out.collect(profile.toJsonString());
                    }

                    private String getTimePeriod(LocalDateTime dateTime) {
                        int hour = dateTime.getHour();
                        if (hour >= 6 && hour < 12) {
                            return "morning";
                        } else if (hour >= 12 && hour < 14) {
                            return "noon";
                        } else if (hour >= 14 && hour < 18) {
                            return "afternoon";
                        } else if (hour >= 18 && hour < 22) {
                            return "evening";
                        } else {
                            return "night";
                        }
                    }
                })
                .name("UserProfileAnalysis");

        // 打印用户画像结果
        userProfileStream.print("用户画像");*/

        System.out.println("🚀 Flink 作业启动成功！开始统计......");
        env.execute("历史天+当天 综合统计");
    }

}
