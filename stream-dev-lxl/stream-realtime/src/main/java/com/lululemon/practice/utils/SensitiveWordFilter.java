package com.stream.realtime.lululemon.practice.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author shuaiqi.chen
 * @date 2025/11/13 15:25
 * @description SensitiveWordFilter
 */
public class SensitiveWordFilter {
    private static Set<String> SENSITIVE_WORDS = new HashSet<>();

    static {
        loadSensitiveWords();
    }

    private static void loadSensitiveWords() {
        try {
            // 从类路径加载敏感词文件
            InputStream inputStream = SensitiveWordFilter.class.getClassLoader()
                    .getResourceAsStream("SensitiveWord/suspected-sensitive-words.txt");

            if (inputStream == null) {
                System.err.println("敏感词文件未找到: suspected-sensitive-words.txt");
                return;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty() && !line.startsWith("#")) {
                        SENSITIVE_WORDS.add(line);
                    }
                }
            }

            System.out.println("成功加载敏感词数量: " + SENSITIVE_WORDS.size());

        } catch (Exception e) {
            System.err.println("加载敏感词文件失败: " + e.getMessage());
        }
    }

    public static String detectLevel(String text) {
        if (text == null || text.isEmpty()) {
            return "P2";
        }

        String lowerText = text.toLowerCase();

        // ===== P0 高优先级：使用从文件加载的敏感词 =====
        for (String word : SENSITIVE_WORDS) {
            if (lowerText.contains(word.toLowerCase())) {
                return "P0";
            }
        }

        // ===== P1 中级：脏话、辱骂、地域歧视、地狱笑话等 =====
        String[] p1Words = {
                "傻逼", "垃圾", "狗屁", "废物", "去死", "脑残", "滚", "蠢货",
                "妈的", "傻子", "畜生", "废柴", "你妈","骗","差劲","无耻","不要脸"
        };
        for (String w : p1Words) {
            if (lowerText.contains(w.toLowerCase())) {
                return "P1";
            }
        }

        // ===== P2 普通评论 =====
        return "P2";
    }

    /**
     * 检测文本中包含的敏感词
     * @param text 待检测的文本
     * @return 命中的敏感词列表，用逗号分隔；如果没有命中则返回空字符串
     */
    public static String detectSensitiveWords(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }

        StringBuilder detectedWords = new StringBuilder();
        String lowerText = text.toLowerCase();

        // 检测文件中的敏感词
        for (String word : SENSITIVE_WORDS) {
            if (lowerText.contains(word.toLowerCase())) {
                if (detectedWords.length() > 0) {
                    detectedWords.append(",");
                }
                detectedWords.append(word);
            }
        }

        // 检测内置的 P1 级别敏感词
        String[] p1Words = {
                "傻逼", "垃圾", "狗屁", "废物", "去死", "脑残", "滚", "蠢货",
                "妈的", "傻子", "畜生", "废柴", "你妈","骗","差劲","无耻","不要脸"
        };
        for (String w : p1Words) {
            if (lowerText.contains(w.toLowerCase())) {
                if (detectedWords.length() > 0) {
                    detectedWords.append(",");
                }
                detectedWords.append(w);
            }
        }

        return detectedWords.toString();
    }

    /**
     * 检查文本是否包含敏感词
     * @param text 待检查的文本
     * @return 如果包含敏感词返回 true，否则返回 false
     */
    public static boolean containsSensitiveWords(String text) {
        if (text == null || text.isEmpty()) {
            return false;
        }

        String lowerText = text.toLowerCase();

        // 检查文件中的敏感词
        for (String word : SENSITIVE_WORDS) {
            if (lowerText.contains(word.toLowerCase())) {
                return true;
            }
        }

        // 检查 P1 级别敏感词
        String[] p1Words = {
                "傻逼", "垃圾", "狗屁", "废物", "去死", "脑残", "滚", "蠢货",
                "妈的", "傻子", "畜生", "废柴", "你妈","骗","差劲","无耻","不要脸"
        };
        for (String w : p1Words) {
            if (lowerText.contains(w.toLowerCase())) {
                return true;
            }
        }

        return false;
    }
}