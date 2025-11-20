package com.stream.realtime.lululemon.practice.func;


import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.*;


/**
 * @author shuaiqi.chen
 * @date 2025/11/19 9:20
 * @description ConsumptionLevelProcessFunction
 */
public class ConsumptionLevelProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {

    // 用于存储用户最近几天的消费记录
    private ValueState<String> userConsumptionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("userConsumption", String.class);
        userConsumptionState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject orderData, Context ctx, Collector<JSONObject> out) throws Exception {
        String userId = orderData.getString("user_id");
        String dateStr = orderData.getString("ds").split(" ")[0]; // 提取日期部分
        double amount = getDoubleValue(orderData, "total_amount");

        Map<String, List<Double>> consumptionMap = new HashMap<>();

        // 从状态中读取现有数据
        String existingData = userConsumptionState.value();
        if (existingData != null && !existingData.isEmpty()) {
            try {
                // 从JSON字符串解析Map，需要处理BigDecimal问题
                JSONObject tempJson = JSONObject.parseObject(existingData);
                consumptionMap = convertJsonToMap(tempJson);
            } catch (Exception e) {
                // 解析失败，使用空map
                consumptionMap = new HashMap<>();
            }
        }

        // 添加当前消费记录
        consumptionMap.computeIfAbsent(dateStr, k -> new ArrayList<>()).add(amount);

        // 保留最近3天的数据
        cleanOldData(consumptionMap, dateStr);

        // 计算消费水平
        String consumptionLevel = calculateConsumptionLevel(consumptionMap);

        // 添加消费水平字段
        orderData.put("consumption_level", consumptionLevel);

        // 将更新后的数据存回状态
        String updatedData = JSONObject.toJSONString(consumptionMap);
        userConsumptionState.update(updatedData);

        out.collect(orderData);
    }

    // 辅助方法：安全获取double值
    private double getDoubleValue(JSONObject json, String field) {
        Object value = json.get(field);
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return 0.0;
    }

    // 辅助方法：将JSON对象转换为Map<String, List<Double>>
    private Map<String, List<Double>> convertJsonToMap(JSONObject json) {
        Map<String, List<Double>> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String date = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                List<?> list = (List<?>) value;
                List<Double> doubleList = new ArrayList<>();
                for (Object item : list) {
                    if (item instanceof BigDecimal) {
                        doubleList.add(((BigDecimal) item).doubleValue());
                    } else if (item instanceof Number) {
                        doubleList.add(((Number) item).doubleValue());
                    } else if (item instanceof String) {
                        try {
                            doubleList.add(Double.parseDouble((String) item));
                        } catch (NumberFormatException e) {
                            doubleList.add(0.0);
                        }
                    } else {
                        doubleList.add(0.0);
                    }
                }
                result.put(date, doubleList);
            }
        }
        return result;
    }

    private void cleanOldData(Map<String, List<Double>> consumptionMap, String currentDate) {
        Set<String> datesToRemove = new HashSet<>();
        for (String date : consumptionMap.keySet()) {
            if (daysBetween(date, currentDate) > 2) { // 只保留最近3天
                datesToRemove.add(date);
            }
        }
        for (String date : datesToRemove) {
            consumptionMap.remove(date);
        }
    }

    private String calculateConsumptionLevel(Map<String, List<Double>> consumptionMap) {
        if (consumptionMap.isEmpty()) {
            return "lower";
        }

        // 检查是否有单日消费超过2500或5000
        boolean hasHighSingleDay = false;
        boolean hasMidSingleDay = false;
        boolean hasLowSingleDay = false;

        List<String> sortedDates = new ArrayList<>(consumptionMap.keySet());
        Collections.sort(sortedDates);

        for (String date : sortedDates) {
            double dailyTotal = 0.0;
            List<Double> amounts = consumptionMap.get(date);
            if (amounts != null) {
                for (Double amount : amounts) {
                    dailyTotal += amount;
                }
            }

            if (dailyTotal > 5000) {
                hasHighSingleDay = true;
            } else if (dailyTotal > 2500) {
                hasMidSingleDay = true;
            } else {
                hasLowSingleDay = true;
            }
        }

        // 检查连续两天消费
        String[] dates = sortedDates.toArray(new String[0]);
        boolean hasHighConsecutive = false;
        boolean hasMidConsecutive = false;

        for (int i = 0; i < dates.length - 1; i++) {
            String date1 = dates[i];
            String date2 = dates[i + 1];

            if (isConsecutiveDays(date1, date2)) {
                double day1Total = 0.0;
                List<Double> day1Amounts = consumptionMap.get(date1);
                if (day1Amounts != null) {
                    for (Double amount : day1Amounts) {
                        day1Total += amount;
                    }
                }

                double day2Total = 0.0;
                List<Double> day2Amounts = consumptionMap.get(date2);
                if (day2Amounts != null) {
                    for (Double amount : day2Amounts) {
                        day2Total += amount;
                    }
                }

                if (day1Total > 5000 && day2Total > 5000) {
                    hasHighConsecutive = true;
                } else if (day1Total > 2500 && day2Total > 2500) {
                    hasMidConsecutive = true;
                }
            }
        }

        // 判断消费水平
        if (hasHighConsecutive || hasHighSingleDay) {
            return "high";
        } else if (hasMidConsecutive || hasMidSingleDay) {
            return "mid";
        } else {
            return "lower";
        }
    }

    private boolean isConsecutiveDays(String date1, String date2) {
        try {
            java.time.LocalDate d1 = java.time.LocalDate.parse(date1);
            java.time.LocalDate d2 = java.time.LocalDate.parse(date2);
            return Math.abs(java.time.temporal.ChronoUnit.DAYS.between(d1, d2)) == 1;
        } catch (Exception e) {
            return false;
        }
    }

    private long daysBetween(String date1, String date2) {
        try {
            java.time.LocalDate d1 = java.time.LocalDate.parse(date1);
            java.time.LocalDate d2 = java.time.LocalDate.parse(date2);
            return Math.abs(java.time.temporal.ChronoUnit.DAYS.between(d1, d2));
        } catch (Exception e) {
            return 0;
        }
    }
}