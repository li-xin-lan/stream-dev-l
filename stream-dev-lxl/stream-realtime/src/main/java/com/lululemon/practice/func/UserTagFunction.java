package com.stream.realtime.lululemon.practice.func;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @author shuaiqi.chen
 * @date 2025/11/19 10:57
 * @description UserTagFunction
 */
public class UserTagFunction implements MapFunction<JSONObject, JSONObject> {

    @Override
    public JSONObject map(JSONObject json) throws Exception {

        // --------------------------
        // 1) 解析生日
        // --------------------------
        String birthday = json.getString("birthday"); // 格式：1994/3/1
        if (birthday != null && birthday.contains("/")) {
            try {
                LocalDate birth = LocalDate.parse(birthday, DateTimeFormatter.ofPattern("yyyy/M/d"));

                int currentYear = 2025; // 你数据里 ds = 2025 年
                int age = currentYear - birth.getYear();
                json.put("age", age);

                // --------------------------
                // 2) 年龄段
                // --------------------------
                json.put("age_group", getAgeGroup(age));

                // --------------------------
                // 3) 年代（80后、90后）
                // --------------------------
                json.put("generation", getGeneration(birth.getYear()));

                // --------------------------
                // 4) 星座
                // --------------------------
                json.put("constellation", getConstellation(birth.getMonthValue(), birth.getDayOfMonth()));

            } catch (Exception e) {
                json.put("age", null);
                json.put("age_group", "未知");
                json.put("generation", "未知");
                json.put("constellation", "未知");
            }
        } else {
            json.put("age", null);
            json.put("age_group", "未知");
            json.put("generation", "未知");
            json.put("constellation", "未知");
        }

        return json;
    }


    // --------------------------
    // 年龄段
    // --------------------------
    private String getAgeGroup(int age) {
        if (age >= 18 && age <= 24) return "18-24岁";
        if (age >= 25 && age <= 29) return "25-29岁";
        if (age >= 30 && age <= 34) return "30-34岁";
        if (age >= 35 && age <= 39) return "35-39岁";
        if (age >= 40 && age <= 49) return "40-49岁";
        if (age >= 50) return "50岁以上";
        return "未知";
    }

    // --------------------------
    // 年代（80后、90后）
    // --------------------------
    private String getGeneration(int birthYear) {
        int decade = birthYear / 10 % 10; // 获取年代

        switch (decade) {
            case 8: return "80后";
            case 9: return "90后";
            case 0: return "00后";
            case 1: return "10后";
            case 7: return "70后";
            default: return "未知";
        }
    }

    // --------------------------
    // 星座计算
    // --------------------------
    private String getConstellation(int month, int day) {

        // 边界日数组
        int[] edgeDay = {20, 19, 21, 20, 21, 22, 23, 23, 23, 24, 23, 22};

        String[] constellations = {
                "水瓶座", "双鱼座", "白羊座", "金牛座",
                "双子座", "巨蟹座", "狮子座", "处女座",
                "天秤座", "天蝎座", "射手座", "摩羯座"
        };

        if (day < edgeDay[month - 1]) {
            // 星座数组下标从 0 开始
            return constellations[(month - 2 + 12) % 12];
        } else {
            return constellations[(month - 1) % 12];
        }
    }
}
