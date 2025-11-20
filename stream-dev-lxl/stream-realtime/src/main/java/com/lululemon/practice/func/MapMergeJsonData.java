package com.stream.realtime.lululemon.practice.func;

import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.stream.realtime.lululemon.formal.func.MapMergeJsonData
 * @Author zhou.han
 * @Date 2025/10/24 18:26
 * @description:
 */
public class MapMergeJsonData extends RichMapFunction<JsonObject, JsonObject> {

    @Override
    public JsonObject map(JsonObject data) throws Exception {
        if (data.has("after") && !data.get("after").isJsonNull()) {

            JsonObject after = data.getAsJsonObject("after").deepCopy();
            JsonObject source = data.getAsJsonObject("source");
            String db = source.has("db") ? source.get("db").getAsString() : "";
            String schema = source.has("schema") ? source.get("schema").getAsString() : "";
            String table = source.has("table") ? source.get("table").getAsString() : "";
            String tableName = db + "." + schema + "." + table;
            String op = data.has("op") ? data.get("op").getAsString() : "";

            after.addProperty("table_name", tableName);
            after.addProperty("op", op);

            return after;
        }

        return null;
    }
}