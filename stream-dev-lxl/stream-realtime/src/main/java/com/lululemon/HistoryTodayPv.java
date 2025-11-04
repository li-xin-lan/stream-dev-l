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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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


    static boolean isChinaRegion(String region) {
        if (region == null || region.isEmpty()) {
            return false;
        }

        // 方式1：检查是否包含"中国"关键字
        if (region.contains("中国")) {
            return true;
        }

        // 方式2：检查具体的中国省份和直辖市
        String[] chinaProvinces = {
                "北京市", "天津市", "上海市", "重庆市",
                "河北省", "山西省", "辽宁省", "吉林省", "黑龙江省",
                "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省",
                "河南省", "湖北省", "湖南省", "广东省", "海南省",
                "四川省", "贵州省", "云南省", "陕西省", "甘肃省",
                "青海省", "台湾省", "内蒙古自治区", "广西壮族自治区",
                "西藏自治区", "宁夏回族自治区", "新疆维吾尔自治区",
                "香港", "澳门"
        };

        for (String province : chinaProvinces) {
            if (region.contains(province)) {
                return true;
            }
        }


        // 方式3：检查地区格式（中国 省份 城市）
        if (region.matches("中国\\s+.*")) {
            return true;
        }

        return false;
    }

        static String extractProvince(String region) {
            if (region == null || region.isEmpty()) {
                return "";
            }

            // 移除"中国"前缀
            String cleanedRegion = region.replace("中国", "").trim();

            // 省份完整名称映射
            Map<String, String> provinceFullMapping = new HashMap<>();
            provinceFullMapping.put("北京市", "北京市");
            provinceFullMapping.put("天津市", "天津市");
            provinceFullMapping.put("上海市", "上海市");
            provinceFullMapping.put("重庆市", "重庆市");
            provinceFullMapping.put("河北省", "河北省");
            provinceFullMapping.put("山西省", "山西省");
            provinceFullMapping.put("辽宁省", "辽宁省");
            provinceFullMapping.put("吉林省", "吉林省");
            provinceFullMapping.put("黑龙江省", "黑龙江省");
            provinceFullMapping.put("江苏省", "江苏省");
            provinceFullMapping.put("浙江省", "浙江省");
            provinceFullMapping.put("安徽省", "安徽省");
            provinceFullMapping.put("福建省", "福建省");
            provinceFullMapping.put("江西省", "江西省");
            provinceFullMapping.put("山东省", "山东省");
            provinceFullMapping.put("河南省", "河南省");
            provinceFullMapping.put("湖北省", "湖北省");
            provinceFullMapping.put("湖南省", "湖南省");
            provinceFullMapping.put("广东省", "广东省");
            provinceFullMapping.put("海南省", "海南省");
            provinceFullMapping.put("四川省", "四川省");
            provinceFullMapping.put("贵州省", "贵州省");
            provinceFullMapping.put("云南省", "云南省");
            provinceFullMapping.put("陕西省", "陕西省");
            provinceFullMapping.put("甘肃省", "甘肃省");
            provinceFullMapping.put("青海省", "青海省");
            provinceFullMapping.put("台湾省", "台湾省");
            provinceFullMapping.put("内蒙古自治区", "内蒙古自治区");
            provinceFullMapping.put("广西壮族自治区", "广西壮族自治区");
            provinceFullMapping.put("西藏自治区", "西藏自治区");
            provinceFullMapping.put("宁夏回族自治区", "宁夏回族自治区");
            provinceFullMapping.put("新疆维吾尔自治区", "新疆维吾尔自治区");
            provinceFullMapping.put("香港特别行政区", "香港");
            provinceFullMapping.put("澳门特别行政区", "澳门");

            // 简写映射
            Map<String, String> provinceShortMapping = new HashMap<>();
            provinceShortMapping.put("北京", "北京市");
            provinceShortMapping.put("天津", "天津市");
            provinceShortMapping.put("上海", "上海市");
            provinceShortMapping.put("重庆", "重庆市");
            provinceShortMapping.put("河北", "河北省");
            provinceShortMapping.put("山西", "山西省");
            provinceShortMapping.put("辽宁", "辽宁省");
            provinceShortMapping.put("吉林", "吉林省");
            provinceShortMapping.put("黑龙江", "黑龙江省");
            provinceShortMapping.put("江苏", "江苏省");
            provinceShortMapping.put("浙江", "浙江省");
            provinceShortMapping.put("安徽", "安徽省");
            provinceShortMapping.put("福建", "福建省");
            provinceShortMapping.put("江西", "江西省");
            provinceShortMapping.put("山东", "山东省");
            provinceShortMapping.put("河南", "河南省");
            provinceShortMapping.put("湖北", "湖北省");
            provinceShortMapping.put("湖南", "湖南省");
            provinceShortMapping.put("广东", "广东省");
            provinceShortMapping.put("海南", "海南省");
            provinceShortMapping.put("四川", "四川省");
            provinceShortMapping.put("贵州", "贵州省");
            provinceShortMapping.put("云南", "云南省");
            provinceShortMapping.put("陕西", "陕西省");
            provinceShortMapping.put("甘肃", "甘肃省");
            provinceShortMapping.put("青海", "青海省");
            provinceShortMapping.put("台湾", "台湾省");
            provinceShortMapping.put("内蒙古", "内蒙古自治区");
            provinceShortMapping.put("广西", "广西壮族自治区");
            provinceShortMapping.put("西藏", "西藏自治区");
            provinceShortMapping.put("宁夏", "宁夏回族自治区");
            provinceShortMapping.put("新疆", "新疆维吾尔自治区");
            provinceShortMapping.put("香港", "香港");
            provinceShortMapping.put("澳门", "澳门");

            // 先检查完整名称
            for (Map.Entry<String, String> entry : provinceFullMapping.entrySet()) {
                if (cleanedRegion.contains(entry.getKey())) {
                    return entry.getValue();
                }
            }

            // 再检查简写
            for (Map.Entry<String, String> entry : provinceShortMapping.entrySet()) {
                if (cleanedRegion.contains(entry.getKey())) {
                    return entry.getValue();
                }
            }

            return "";
        }
        // ============ 省份提取方法结束 ============



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

            // 将集合转换为逗号分隔的字符串
            json.put("login_dates", String.join(",", login_days));
            json.put("login_days_count", login_days.size());
            json.put("login_periods", String.join(",", login_periods));
            json.put("has_purchase", String.valueOf(has_purchase));
            json.put("has_search", String.valueOf(has_search));
            json.put("has_view", String.valueOf(has_view));

            // 计算首次和最后登录日期
            if (!login_days.isEmpty()) {
                List<String> sortedDays = new ArrayList<>(login_days);
                Collections.sort(sortedDays);
                json.put("first_login_date", sortedDays.get(0));
                json.put("last_login_date", sortedDays.get(sortedDays.size() - 1));
            } else {
                json.put("first_login_date", "");
                json.put("last_login_date", "");
            }

            // 格式化更新时间
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            json.put("update_time", sdf.format(new Date(update_time)));

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
        Configuration config = new Configuration();
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

        dailyPagePv.print();*/

        // ==================== 页面访问量统计写入 Doris ====================
        /*JdbcConnectionOptions pagePvJdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://172.22.78.0:9030/bigdata_realtime_lululemon_user_portrait")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

// 创建页面访问量统计的 JDBC Sink
        SinkFunction<Tuple3<String, String, Long>> pagePvSink = JdbcSink.sink(
                "INSERT INTO page_pv_statistics(stat_date, page_type, pv_count, update_time) " +
                        "VALUES (?, ?, ?, ?)",
                (statement, tuple) -> {
                    try {
                        statement.setString(1, tuple.f0); // stat_date
                        statement.setString(2, tuple.f1); // page_type
                        statement.setLong(3, tuple.f2);   // pv_count
                        statement.setString(4, LocalDateTime.now().format(TIME_FORMATTER)); // update_time
                    } catch (Exception e) {
                        System.err.println("页面访问量数据写入错误: " + tuple);
                        e.printStackTrace();
                    }
                },
                pagePvJdbcOptions);

// 添加页面访问量统计 Sink
        dailyPagePv.addSink(pagePvSink)
                .name("JdbcSink-PagePV")
                .setParallelism(1);*/

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

// 累加每个搜索词的总次数
        DataStream<Tuple2<String, Long>> countStream = keywordStream
                .keyBy(t -> t.f0)
                .sum(1);

// 搜索词TOP10处理 - 输出结构化数据
        SingleOutputStreamOperator<String> keyWorksTop10 = countStream
                .keyBy(t -> 0) // 全局排序
                .process(new KeyedProcessFunction<Integer, Tuple2<String, Long>, String>() {

                    private final Map<String, Long> counts = new HashMap<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) {
                        counts.put(value.f0, value.f1);

                        // 取前10并构建结构化数据
                        List<Map<String, Object>> top10List = counts.entrySet()
                                .stream()
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .limit(10)
                                .map(entry -> {
                                    Map<String, Object> item = new HashMap<>();
                                    item.put("keyword", entry.getKey());
                                    item.put("search_count", entry.getValue());
                                    return item;
                                })
                                .collect(Collectors.toList());

                        // 构建结构化JSON
                        JSONObject result = new JSONObject();
                        result.put("top10_list", top10List);
                        result.put("update_time", LocalDateTime.now().format(TIME_FORMATTER));
                        result.put("total_keywords", counts.size());

                        out.collect(result.toJSONString());
                    }
                });

        keyWorksTop10.print();

// ==================== TOP10搜索词排名写入 Doris ====================
        JdbcConnectionOptions keywordTop10JdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://172.22.78.0:9030/bigdata_realtime_lululemon_user_portrait")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

// 创建TOP10搜索词排名的 JDBC Sink
        SinkFunction<String> keywordTop10Sink = JdbcSink.sink(
                "INSERT INTO keyword_top10_rankings(keyword, search_count, ranking, update_time) " +
                        "VALUES (?, ?, ?, ?)",
                (statement, jsonData) -> {
                    try {
                        JSONObject json = JSON.parseObject(jsonData);
                        JSONArray top10List = json.getJSONArray("top10_list");

                        // 清空旧数据（可选，根据需求决定）
                        // 或者使用REPLACE INTO语句

                        // 插入TOP10数据
                        for (int i = 0; i < top10List.size(); i++) {
                            JSONObject item = top10List.getJSONObject(i);
                            statement.setString(1, item.getString("keyword"));      // keyword
                            statement.setLong(2, item.getLong("search_count"));     // search_count
                            statement.setInt(3, i + 1);                            // ranking
                            statement.setString(4, json.getString("update_time")); // update_time
                            statement.addBatch();
                        }
                        statement.executeBatch();

                    } catch (Exception e) {
                        System.err.println("TOP10搜索词排名写入错误: " + jsonData);
                        e.printStackTrace();
                    }
                },
                keywordTop10JdbcOptions);

// 添加TOP10搜索词排名 Sink
        keyWorksTop10.addSink(keywordTop10Sink)
                .name("JdbcSink-KeywordTop10")
                .setParallelism(1);*/



// TODO: 2025/11/3 需求3 登录区域热力（IP转地址）



        /*DataStream<Tuple2<String, Long>> regionStream = kafkaStream
                .flatMap((String json, Collector<Tuple2<String, Long>> out) -> {
                    try {
                        JsonNode node = mapper.readTree(json);
                        if (node.has("log_type") && "login".equals(node.get("log_type").asText())
                                && node.has("region")) {
                            String region = node.get("region").asText().trim();

                            // 添加过滤条件：只保留中国地区
                            if (!region.isEmpty() && isChinaRegion(region)) {
                                // 提取省份信息
                                String province = extractProvince(region);
                                if (!province.isEmpty()) {
                                    out.collect(new Tuple2<>(province, 1L));
                                }
                            }
                        }
                    } catch (Exception e) {
                        // 忽略解析异常
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

// 按省份累加
        DataStream<Tuple2<String, Long>> regionCountStream = regionStream
                .keyBy(t -> t.f0)
                .sum(1).map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        System.out.println("准备写入Doris: 省份=" + value.f0 + ", 次数=" + value.f1);
                        return value;
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));

        regionCountStream.print();
// ==================== 地区统计写入 Doris ====================
        JdbcConnectionOptions regionJdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://172.22.78.0:9030/bigdata_realtime_lululemon_user_portrait")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

// 创建地区统计的 JDBC Sink
        SinkFunction<Tuple2<String, Long>> regionSink = JdbcSink.sink(
                "INSERT INTO region_login_statistics(country, province, login_count, update_time) " +
                        "VALUES (?, ?, ?, ?)",
                (statement, tuple) -> {
                    try {
                        statement.setString(1, "中国"); // country
                        statement.setString(2, tuple.f0); // province
                        statement.setLong(3, tuple.f1); // login_count
                        statement.setString(4, LocalDateTime.now().format(TIME_FORMATTER)); // update_time
                    } catch (Exception e) {
                        System.err.println("地区统计写入错误: " + tuple);
                        e.printStackTrace();
                    }
                },
                regionJdbcOptions);

// 添加地区统计 Sink
        regionCountStream.addSink(regionSink)
                .name("JdbcSink-RegionLogin")
                .setParallelism(1);*/


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


        DataStream<Tuple2<String, Long>> devicePlatformStream = kafkaStream
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
                            out.collect(new Tuple2<>("platform_type:" + platformType, 1L));

                            // 统计品牌（按平台分类）- 过滤掉品牌为"其他"的数据
                            if (!brand.isEmpty() && !"其他".equals(brand)) {
                                out.collect(new Tuple2<>("brand:" + platformType + ":" + brand, 1L));
                            }

                            // 统计设备型号（按平台分类）- 过滤掉无法识别品牌的设备
                            if (!device.isEmpty()) {
                                // 从设备型号推断品牌，如果是"其他"则跳过
                                String inferredBrand = inferBrandFromDevice(device, platformType);
                                if (!"其他".equals(inferredBrand)) {
                                    out.collect(new Tuple2<>("device_model:" + platformType + ":" + device, 1L));
                                }
                            }

                            // 统计平台版本（按平台分类）
                            if (!platv.isEmpty()) {
                                out.collect(new Tuple2<>("platform_version:" + platformType + ":" + platv, 1L));
                            }

                            // 统计软件版本（按平台分类）
                            if (!softv.isEmpty()) {
                                out.collect(new Tuple2<>("software_version:" + platformType + ":" + softv, 1L));
                            }

                        } catch (Exception e) {
                            // 忽略解析异常
                        }
                    }

                    // 从设备型号推断品牌的辅助方法
                    private String inferBrandFromDevice(String device, String platform) {
                        if (platform.equals("iOS")) {
                            return "苹果";
                        }

                        String deviceLower = device.toLowerCase();
                        if (deviceLower.contains("huawei") || deviceLower.contains("honor")) {
                            return "华为";
                        } else if (deviceLower.contains("xiaomi") || deviceLower.contains("mi ") || deviceLower.contains("redmi")) {
                            return "小米";
                        } else if (deviceLower.contains("oppo")) {
                            return "OPPO";
                        } else if (deviceLower.contains("vivo")) {
                            return "VIVO";
                        } else if (deviceLower.contains("samsung")) {
                            return "三星";
                        } else if (deviceLower.contains("oneplus")) {
                            return "一加";
                        } else if (deviceLower.contains("realme")) {
                            return "Realme";
                        } else {
                            return "其他";
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
                        for (int i = 0; i < 70; i++) {
                            sb.append("=");
                        }
                        sb.append("\n");

                        // 平台类型分布
                        appendPlatformTypeReport(sb);

                        // 详细设备统计（按平台分组）
                        appendDetailedDeviceReport(sb);

                        for (int i = 0; i < 70; i++) {
                            sb.append("=");
                        }
                        sb.append("\n");
                        return sb.toString();
                    }

                    private void appendPlatformTypeReport(StringBuilder sb) {
                        long iosCount = devicePlatformStats.getOrDefault("platform_type:iOS", 0L);
                        long androidCount = devicePlatformStats.getOrDefault("platform_type:Android", 0L);
                        long otherCount = devicePlatformStats.getOrDefault("platform_type:其他", 0L);

                        long total = iosCount + androidCount + otherCount;

                        if (total == 0) {
                            sb.append("📊 平台类型分布: 暂无数据\n\n");
                            return;
                        }

                        double iosPercent = total > 0 ? (iosCount * 100.0 / total) : 0;
                        double androidPercent = total > 0 ? (androidCount * 100.0 / total) : 0;
                        double otherPercent = total > 0 ? (otherCount * 100.0 / total) : 0;

                        sb.append("📊 平台类型分布:\n");
                        sb.append(String.format("  iOS     : %6d次 (%.1f%%)\n", iosCount, iosPercent));
                        sb.append(String.format("  Android : %6d次 (%.1f%%)\n", androidCount, androidPercent));
                        sb.append(String.format("  其他    : %6d次 (%.1f%%)\n", otherCount, otherPercent));
                        sb.append("\n");
                    }

                    private void appendDetailedDeviceReport(StringBuilder sb) {
                        sb.append("📱 详细设备统计:\n");

                        // 收集所有品牌和设备型号数据
                        List<DeviceInfo> allDevices = new ArrayList<>();

                        // 处理iOS设备
                        processPlatformDevices(allDevices, "iOS");

                        // 处理Android设备
                        processPlatformDevices(allDevices, "Android");

                        // 处理其他平台设备
                        processPlatformDevices(allDevices, "其他");

                        // 按次数排序
                        allDevices.sort((a, b) -> Long.compare(b.count, a.count));

                        // 输出所有设备信息
                        for (DeviceInfo device : allDevices) {
                            sb.append(String.format("  系统：%-8s, 品牌：%-10s, 型号：%-20s, 次数：%d\n",
                                    device.platform, device.brand, device.model, device.count));
                        }

                        if (allDevices.isEmpty()) {
                            sb.append("  暂无详细设备数据\n");
                        }
                        sb.append("\n");
                    }

                    private void processPlatformDevices(List<DeviceInfo> allDevices, String platform) {
                        // 处理品牌数据
                        for (Map.Entry<String, Long> entry : devicePlatformStats.entrySet()) {
                            if (entry.getKey().startsWith("brand:" + platform + ":")) {
                                String brand = entry.getKey().split(":" + platform + ":")[1];
                                allDevices.add(new DeviceInfo(platform, brand, "未知", entry.getValue()));
                            }
                        }

                        // 处理设备型号数据
                        for (Map.Entry<String, Long> entry : devicePlatformStats.entrySet()) {
                            if (entry.getKey().startsWith("device_model:" + platform + ":")) {
                                String model = entry.getKey().split(":" + platform + ":")[1];
                                // 从型号推断品牌
                                String brand = inferBrandFromModel(model, platform);
                                allDevices.add(new DeviceInfo(platform, brand, model, entry.getValue()));
                            }
                        }
                    }

                    private String inferBrandFromModel(String model, String platform) {
                        if (platform.equals("iOS")) {
                            return "苹果";
                        }

                        String modelLower = model.toLowerCase();
                        if (modelLower.contains("huawei") || modelLower.contains("honor")) {
                            return "华为";
                        } else if (modelLower.contains("xiaomi") || modelLower.contains("mi ") || modelLower.contains("redmi")) {
                            return "小米";
                        } else if (modelLower.contains("oppo")) {
                            return "OPPO";
                        } else if (modelLower.contains("vivo")) {
                            return "VIVO";
                        } else if (modelLower.contains("samsung")) {
                            return "三星";
                        } else if (modelLower.contains("oneplus")) {
                            return "一加";
                        } else if (modelLower.contains("realme")) {
                            return "Realme";
                        } else {
                            return "其他";
                        }
                    }

                    // 设备信息辅助类
                    class DeviceInfo {
                        String platform;
                        String brand;
                        String model;
                        long count;

                        DeviceInfo(String platform, String brand, String model, long count) {
                            this.platform = platform;
                            this.brand = brand;
                            this.model = model;
                            this.count = count;
                        }
                    }
                })
                .name("DevicePlatformAnalysisReport");

        devicePlatformAnalysis.print("用户设备平台统计");

        JdbcConnectionOptions deviceJdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://172.22.78.0:9030/bigdata_realtime_lululemon_user_portrait")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

// 创建设备统计的 JDBC Sink
/*        SinkFunction<Tuple2<String, Long>> deviceSink = JdbcSink.sink(
                "INSERT INTO device_platform_stats(stat_type, stat_key, stat_value, platform_type, update_time) " +
                        "VALUES (?, ?, ?, ?, ?)",
                (statement, tuple) -> {
                    try {
                        String key = tuple.f0;
                        Long count = tuple.f1;
                        String updateTime = LocalDateTime.now().format(TIME_FORMATTER);

                        // 解析统计类型和平台类型
                        String[] parts = key.split(":");
                        String statType = parts[0];
                        String platformType = parts.length > 1 ? parts[1] : "";
                        String statKey = parts.length > 2 ? parts[2] : platformType;

                        statement.setString(1, statType);
                        statement.setString(2, statKey);
                        statement.setLong(3, count);
                        statement.setString(4, platformType);
                        statement.setString(5, updateTime);

                    } catch (Exception e) {
                        System.err.println("设备统计写入错误: " + tuple);
                        System.err.println("错误信息: " + e.getMessage());
                        e.printStackTrace();
                    }
                },
                deviceJdbcOptions);

// 添加设备统计 Sink
        devicePlatformCountStream.addSink(deviceSink)
                .name("JdbcSink-DeviceStats")
                .setParallelism(1);*/



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
                            return "6:00-12:00";
                        } else if (hour >= 12 && hour < 18) {
                            return "12:00-18:00";
                        } else if (hour >= 18 && hour < 22) {
                            return "18:00-22:00";
                        } else {
                            return "22:00-6:00";
                        }
                    }
                })
                .name("UserProfileAnalysis");

        // 打印用户画像结果
        userProfileStream.print("用户画像");*/



        /*JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://172.22.78.0:9030/bigdata_realtime_lululemon_user_portrait")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

// 创建 JDBC Sink - 使用 Doris 支持的 INSERT 语法
        SinkFunction<String> jdbcSink = JdbcSink.sink(
                "INSERT INTO user_profile(user_id, login_dates, login_days_count, login_periods, has_purchase, has_search, has_view, first_login_date, last_login_date, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, jsonStr) -> {
                    // 解析 JSON 并设置参数
                    JSONObject json = JSON.parseObject(jsonStr);
                    statement.setString(1, json.getString("user_id"));
                    statement.setString(2, json.getString("login_dates"));
                    statement.setInt(3, json.getIntValue("login_days_count"));
                    statement.setString(4, json.getString("login_periods"));
                    statement.setString(5, json.getString("has_purchase"));
                    statement.setString(6, json.getString("has_search"));
                    statement.setString(7, json.getString("has_view"));
                    statement.setString(8, json.getString("first_login_date"));
                    statement.setString(9, json.getString("last_login_date"));
                    statement.setString(10, json.getString("update_time"));
                },
                jdbcOptions);

// 添加 Sink 到数据流
        userProfileStream.addSink(jdbcSink)
                .name("JdbcSink-UserProfile");*/






        System.out.println("🚀 Flink 作业启动成功！开始统计......");
        env.execute("历史天+当天 综合统计");
    }

}
