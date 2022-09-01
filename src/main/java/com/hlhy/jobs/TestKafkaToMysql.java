package com.hlhy.jobs;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.*;


public class TestKafkaToMysql {

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
        StreamStatementSet stmtSet  = tableEnv.createStatementSet();
        //增加一些配置参数
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer","drop");

        //第一步建source表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `source`(\n" +
                "  `id` BIGINT COMMENT 'id',\n" +
                "  `name` STRING COMMENT '名字'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'bigdata01:9092,bigdata02:9092,bigdata03:9092',\n" +
                "  'topic' = 'test_ad_mini_report_20220808',\n" +
                "  'properties.group.id' = 'test_ad_mini_report_20220808',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")");

        //第二步创建sink1表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `sink1`(\n" +
                "  `id` BIGINT NOT NULL COMMENT 'id',\n" +
                "  `pv` BIGINT NOT NULL COMMENT 'pv',\n" +
                "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/ad',\n" +
                "   'driver'= 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'test_20220824',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '',\n" +
                "   'sink.buffer-flush.max-rows'='10',\n" +
                "   'sink.buffer-flush.interval'='5s'\n" +
                ")");

        //导入程序
        stmtSet.addInsertSql("INSERT INTO sink1 select id,count(name) as pv  from source group by id");

        //创建sink2表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `sink2`(\n" +
                "  `id` BIGINT NOT NULL COMMENT 'id',\n" +
                "  `uv` BIGINT NOT NULL COMMENT 'uv',\n" +
                "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/ad?useUnicode=true&characterEncoding=UTF-8',\n" +
                "   'driver'= 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'test_20220824',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '',\n" +
                "   'sink.buffer-flush.max-rows'='10',\n" +
                "   'sink.buffer-flush.interval'='5s'\n" +
                ")");

        //导入程序
        stmtSet.addInsertSql("INSERT INTO sink2 select id,count(distinct name) as uv  from source group by id");

        // 执行刚刚添加的所有 INSERT 语句
        TableResult tableResult = stmtSet.execute();
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult.getJobClient().get().getJobStatus());

    }
}
