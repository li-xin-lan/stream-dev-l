package com.stream.realtime.lululemon.practice.func;

/**
 * @author shuaiqi.chen
 * @date 2025/11/18 19:48
 * @description CleanHbaseValue
 */
public class CleanHbaseValue {
    public static String clean(String v) {
        if (v == null) return null;

        v = v.trim();

        if (v.startsWith("\"") && v.endsWith("\"")) {
            v = v.substring(1, v.length() - 1);
        }

        v = v.replace("\\\"", "\"");

        return v;
    }
}
