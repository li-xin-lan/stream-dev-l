package com.lxl;

import common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: lxl
 * @Date: 2025/9/26 🐕 🎇
 */
public class Test {
        
        @SneakyThrows
        public static void main(String[]args) {
            System.setProperty("HADOOP_USER_NAME", "root");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            EnvironmentSettingUtils.defaultParameter(env);
            //
            //

            DataStreamSource<String> dataStreamSource = env.socketTextStream("cdh01", 10256);
            dataStreamSource.print();
            env.execute();
        }

}
