package com.stream.realtime.lululemon.practice.func;

import com.alibaba.fastjson2.JSONObject;

import com.stream.core.utils.HbaseUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

/**
 * @author shuaiqi.chen
 * @date 2025/11/18 16:12
 * @description ReadHbaseData
 *              Code Comment: 工单编号 - 大数据-用户画像-11-达摩盘基础特征
 *              功能：读取 HBase 数据，清理异常引号，并打印。
 */
public class ReadHbaseData {

    public static SourceFunction<JSONObject> buildHbaseSource(
            String zkQuorum,
            String tableName,
            long limit
    ) {
        return new SourceFunction<JSONObject>() {

            private volatile boolean running = true;

            @SneakyThrows
            @Override
            public void run(SourceContext<JSONObject> ctx) throws Exception {

                HbaseUtils hbaseUtils = new HbaseUtils(zkQuorum);

                ArrayList<JSONObject> list = hbaseUtils.getAll(tableName, limit);

                for (JSONObject js : list) {
                    if (!running) break;
                    ctx.collect(js);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        };
    }
}