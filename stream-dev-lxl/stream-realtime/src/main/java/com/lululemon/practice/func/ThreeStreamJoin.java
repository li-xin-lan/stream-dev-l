package com.stream.realtime.lululemon.practice.func;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author shuaiqi.chen
 * @date 2025/11/18 19:49
 * @description ThreeStreamJoin
 */
public class ThreeStreamJoin extends KeyedProcessFunction<String, JSONObject, JSONObject> {
    private ValueState<JSONObject> hbaseState;
    private ValueState<JSONObject> kafkaState;
    private ValueState<JSONObject> sqlState;

    @Override
    public void open(Configuration parameters) {

        hbaseState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("hbase", JSONObject.class)
        );

        kafkaState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("kafka", JSONObject.class)
        );

        sqlState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sqlserver", JSONObject.class)
        );
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

        String src = value.getString("src");

        if ("hbase".equals(src)) {
            hbaseState.update(value.getJSONObject("hbase_data"));
        } else if ("kafka".equals(src)) {
            kafkaState.update(value.getJSONObject("kafka_data"));
        } else if ("sqlserver".equals(src)) {
            sqlState.update(value.getJSONObject("sql_data"));
        }

        // 三流都准备好才输出
        if (hbaseState.value() != null &&
                kafkaState.value() != null &&
                sqlState.value() != null) {

            JSONObject result = new JSONObject();
            result.put("user_id", ctx.getCurrentKey());
            result.put("hbase", hbaseState.value());
            result.put("kafka", kafkaState.value());
            result.put("sqlserver", sqlState.value());

            out.collect(result);
        }
    }
}
