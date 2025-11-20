package com.stream.realtime.lululemon.practice.func;

import com.alibaba.fastjson2.JSONObject;
import com.stream.realtime.lululemon.practice.utils.SensitiveWordFilter;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @author shuaiqi.chen
 * @date 2025/11/13 15:05
 * @description commentJsonAfter
 */
public class MapSqlServerData extends RichMapFunction<JSONObject,JSONObject> {

    @Override
    public JSONObject map(JSONObject data) throws Exception {
        if (data.containsKey("after") && data.getJSONObject("after") != null){
            JSONObject after = data.getJSONObject("after");
            // --- 新增逻辑结束 ---

            // 添加敏感词过滤
            if (after.containsKey("comment")) {
                String originalComment = after.getString("comment");
                if (originalComment != null && !originalComment.isEmpty()) {
                    // 检测敏感内容等级
                    String level = SensitiveWordFilter.detectLevel(originalComment);
                    after.put("comment_level", level);

                    // 检测并记录命中的敏感词 - 只有在有敏感词时才添加字段
                    String detectedSensitiveWords = SensitiveWordFilter.detectSensitiveWords(originalComment);
                    if (detectedSensitiveWords != null && !detectedSensitiveWords.isEmpty()) {
                        after.put("sensitive_words", detectedSensitiveWords);
                    }
                    // 如果没有敏感词，不添加 sensitive_words 字段
                } else {
                    // 如果评论为空，设置默认值
                    after.put("comment_level", "P2");
                    // 不添加 sensitive_words 字段
                }
            } else {
                // 如果没有评论字段，设置默认值
                after.put("comment_level", "P2");
                // 不添加 sensitive_words 字段
            }
            return after;
        }
        return null;
    }
}