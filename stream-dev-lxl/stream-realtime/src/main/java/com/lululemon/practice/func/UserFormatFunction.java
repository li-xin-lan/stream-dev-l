package com.stream.realtime.lululemon.practice.func;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Arrays;

public class UserFormatFunction implements MapFunction<JSONObject, JSONObject> {

    @Override
    public JSONObject map(JSONObject src) throws Exception {

        JSONObject out = new JSONObject();

        // -------------------------
        // 1. 基本信息
        // -------------------------
        out.put("userid", src.getString("user_id"));
        out.put("username", src.getString("uname"));

        // -------------------------
        // 2. user_base_info
        // -------------------------
        JSONObject baseInfo = new JSONObject();
        baseInfo.put("birthday", src.getString("birthday"));
        baseInfo.put("decade", src.getString("generation"));
        baseInfo.put("gender", convertGender(src.getString("gender"))); // 0=女 1=男
        baseInfo.put("zodiac_sign", src.getString("constellation"));
        baseInfo.put("weight", "");
        baseInfo.put("age", src.getInteger("age"));
        baseInfo.put("age_group", src.getString("age_group"));
        out.put("user_base_info", baseInfo);

        // -------------------------
        // 3. login_time
        // -------------------------
        out.put("login_time", new JSONArray(Arrays.asList(src.getString("ds"))));

        // -------------------------
        // 4. 消费水平
        // -------------------------
        out.put("consumption_level", src.getString("consumption_level"));

        // -------------------------
        // 5. 设备信息
        // -------------------------
        out.put("device_info", src.getJSONObject("device"));

        // -------------------------
        // 6. 搜索信息
        // -------------------------
        JSONObject searchInfo = new JSONObject();
        searchInfo.put("keywords", src.getJSONArray("keywords"));
        searchInfo.put("opa", src.getString("opa"));
        searchInfo.put("log_type", src.getString("log_type"));
        out.put("search_info", searchInfo);

        // -------------------------
        // 7. category_info
        // -------------------------
        JSONObject category = new JSONObject();
        category.put("category_name", src.getString("category_name"));
        category.put("product_name", src.getString("product_name"));
        category.put("goods_name", src.getString("goods_name"));
        out.put("category_info", category);

        // -------------------------
        // 8. shopping_gender（修复：gender 转换为 男/女）
        // -------------------------
        JSONObject shopping = new JSONObject();
        shopping.put("gender", convertGender(src.getString("gender")));  // ★ 修改在这里
        shopping.put("shopping_id",
                new JSONArray(Arrays.asList(src.getString("order_id"))));
        out.put("shopping_gender", shopping);

        // -------------------------
        // 9. 敏感词处理
        // -------------------------
        JSONArray sensitiveArr = new JSONArray();
        String sensitiveWords = src.getString("sensitive_words");

        if (sensitiveWords != null && sensitiveWords.length() > 0) {
            out.put("is_check_sensitive_comment", "1");

            String[] arr = sensitiveWords.split(",");
            for (String w : arr) {
                JSONObject item = new JSONObject();
                item.put("trigger_time", src.getString("ds"));
                item.put("trigger_word", w);
                item.put("orderid", src.getString("order_id"));
                item.put("level", src.getString("comment_level"));
                sensitiveArr.add(item);
            }
        } else {
            out.put("is_check_sensitive_comment", "0");
        }
        out.put("sensitive_word", sensitiveArr);

        // -------------------------
        // 10. 原始字段
        // -------------------------
        out.put("ds", src.getString("ds"));
        out.put("ts", src.getString("ts"));

        return out;
    }

    private String convertGender(String g) {
        if (g == null) return "未知";
        if (g.equals("0")) return "女";
        if (g.equals("1")) return "男";
        return "未知";
    }
}