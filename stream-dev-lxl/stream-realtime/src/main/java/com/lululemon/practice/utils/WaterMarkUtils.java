package com.stream.realtime.lululemon.practice.utils;

import com.alibaba.fastjson2.JSONObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * @author weikaijun
 * @date 2022-07-05 15:34
 **/
@Slf4j
public class WaterMarkUtils {

    private static final Logger logger = LoggerFactory.getLogger(WaterMarkUtils.class);

    public static WatermarkStrategy<JSONObject> getEthWarnWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((record, ts) -> {
                    long time;
                    time = record.containsKey("block_timestamp") ? record.getLong("block_timestamp") : record.getLong("timestamp");
                    return time * 1000;
                });
    }

    public static WatermarkStrategy<List<JSONObject>> getEthLiquidityWaterMark(long durationSeconds) {
        return WatermarkStrategy
                .<List<JSONObject>>forBoundedOutOfOrderness(Duration.ofSeconds(durationSeconds))
                .withTimestampAssigner((list, ts) -> {
                    JSONObject record = list.get(0);
                    return record.getLong("window_start_time");
                });
    }

    public static WatermarkStrategy<String> publicAssignWatermarkStrategy(String timestampField, long maxOutOfOrderlessSeconds) {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderlessSeconds))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        JSONObject jsonObject = JSONObject.parseObject(event);
                        if (event != null && jsonObject.containsKey(timestampField)) {
                            return jsonObject.getLong(timestampField);
                        }
                        return 0L;
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("Failed to parse event or get field '" + timestampField + "': " + event);
                        return 0L;
                    }
                });
    }

    public static WatermarkStrategy<String> publicAssignWatermarkStrategyUseGsonParse(String timestampField, long maxOutOfOrderlessSeconds) {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(maxOutOfOrderlessSeconds))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        JsonObject jsonObject = JsonParser.parseString(event).getAsJsonObject();
                        if (jsonObject.has(timestampField)) {
                            JsonElement timestampElement = jsonObject.get(timestampField);

                            // 处理可能包含小数点的时间戳
                            if (timestampElement.isJsonPrimitive() && timestampElement.getAsJsonPrimitive().isString()) {
                                String tsStr = timestampElement.getAsString();
                                if (tsStr.contains(".")) {
                                    // 如果包含小数点，转换为 long（取整数部分）
                                    double doubleValue = Double.parseDouble(tsStr);
                                    return (long) doubleValue;
                                } else {
                                    return Long.parseLong(tsStr);
                                }
                            } else if (timestampElement.isJsonPrimitive() && timestampElement.getAsJsonPrimitive().isNumber()) {
                                // 如果是数字类型，直接获取 long 值
                                return timestampElement.getAsLong();
                            }
                        }
                        return 0L;
                    } catch (JsonSyntaxException | NumberFormatException | IllegalStateException e) {
                        e.printStackTrace();
                        logger.error("Failed to parse event or get field '" + timestampField + "': " + event + ", error: " + e.getMessage());
                        return 0L;
                    }
                });
    }

}