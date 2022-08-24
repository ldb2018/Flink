package com.hlhy.jobs;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class MysqlCDC {
    public static void main(String[] args) {
//TODO 1.创建Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
//开启CK
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

//Flink表环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `device_install_active_info`(\n" +
                "  `game_code` STRING NOT NULL,\n" +
                "  `distinct_id` STRING NOT NULL,\n" +
                "  `last_active_time` STRING  NULL,\n" +
                "  `jsonstr` STRING  NULL,\n" +
                "  PRIMARY KEY(`game_code`,`distinct_id`) NOT ENFORCED\n" +
                ") with (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '127.0.0.1',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                "  'database-name' = 'ad',\n" +
                "  'table-name' = 'device_install_active_info',\n" +
                "  'scan.startup.mode' = 'initial'\n" +
                ")");

        // 将数据导入hive
        tableEnv.executeSql("select * from  `device_install_active_info` ").print();
    }
}
