package com.hlhy.jobs;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class TestAdMatchReport {

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
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS kafka_json_source_view (\n" +
                "   stat_day   DATE ,\n" +
                "   ad_uniqid   STRING ,\n" +
                "   client     STRING ,\n" +
                "   os     STRING ,\n" +
                "   game_code         STRING ,\n" +
                "   distinct_id         STRING ,\n" +
                "   account_id         STRING ,\n" +
                "   ad_type         BIGINT ,\n" +
                "   ad_plat         STRING ,\n" +
                "   ad_time         STRING ,\n" +
                "   campaign_id  STRING ,\n" +
                "   ad_id  STRING ,\n" +
                "   adcreative_id  STRING ,\n" +
                "   last_active_time         STRING ,\n" +
                "   last_active_event         STRING ,\n" +
                "   first_time_active_time         STRING ,\n" +
                "   match_state  STRING ,\n" +
                "   monitor_id         BIGINT ,\n" +
                "   package_id         STRING ,\n" +
                "   register_time  STRING ,\n" +
                "   first_pay_time  STRING ,\n" +
                "   last_pay_amount         BIGINT ,\n" +
                "   role_id  STRING,\n" +
                "   first_create_game_role_time  STRING    \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'properties.bootstrap.servers' = 'emr_kafka1:9092,emr_kafka2:9092,emr_kafka3:9092',\n" +
                "    'topic' = 'ad_match_result_report_view',\n" +
                "    'properties.group.id' = 'ad_match_result_report_view_groupid_test',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'csv',\n" +
                "    'csv.field-delimiter'='\\t',\n" +
                "    'csv.ignore-parse-errors'='true',\n" +
                "    'csv.disable-quote-character' ='true'\n" +
                ")");

        //第二步创建sink1表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `realtime_overview_hour`(\n" +
                "  `stat_day` DATE NOT NULL COMMENT '统计日期',\n" +
                "  `stat_hour` BIGINT  NOT NULL COMMENT  '统计小时',\n" +
                "  `ad_uniqid` STRING NOT NULL COMMENT '广告唯一id, 视小程序或app投放不同',\n" +
                "  `client` STRING NOT NULL COMMENT 'app或mp',\n" +
                "  `monitor_id` BIGINT  NULL COMMENT '监测id',\n" +
                "  `campaign_id` STRING  NULL COMMENT '广告组id',\n" +
                "  `ad_id` STRING  NULL COMMENT '广告计划id',\n" +
                "  `adcreative_id` STRING  NULL COMMENT '创意id',\n" +
                "  `active_user` BIGINT NULL COMMENT '活跃用户数',\n" +
                "  `new_active` BIGINT  NULL COMMENT '新增激活',\n" +
                "  `new_reg` BIGINT  NULL COMMENT '新增注册',\n" +
                "  `new_account` BIGINT   NULL COMMENT '新增账号',\n" +
                "  `new_create_role` BIGINT   NULL COMMENT '新增创角',\n" +
                "  `new_create_role_all` BIGINT   NULL COMMENT '新增创角(全)',\n" +
                "  `first_create_role` BIGINT   NULL COMMENT '首次创角',\n" +
                "  `new_pay_user` BIGINT   NULL COMMENT '新增付费用户数',\n" +
                "  `new_pay_amount` BIGINT   NULL COMMENT '新增充值',\n" +
                "  `new_pay_amount_android` BIGINT   NULL COMMENT '新增充值-安卓',\n" +
                "  `new_pay_amount_ios` BIGINT   NULL COMMENT '新增充值-IOS',\n" +
                "  `first_pay_amount` BIGINT   NULL COMMENT '单设备新增付费账号新增充值 不管注册是哪天 只看是不是今天首充付费',\n" +
                "  `new_pay_user_by_device` BIGINT   NULL COMMENT '单设备新增付费账号 不管注册是哪天 只看是不是今天首充付费',\n" +
                "  PRIMARY KEY (`stat_day`,`stat_hour`,`ad_uniqid`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/ad',\n" +
                "   'driver'= 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'realtime_overview_hour',\n" +
                "   'username' = 'root',\n" +
                "   'password' = ''\n" +
                ")");

        //导入程序
        stmtSet.addInsertSql("INSERT INTO `realtime_overview_hour`(stat_day,stat_hour,ad_uniqid,client,monitor_id,campaign_id,ad_id,adcreative_id,active_user,new_active,new_reg,new_account,new_create_role,new_create_role_all,first_create_role,new_pay_user,new_pay_amount,new_pay_amount_android,new_pay_amount_ios,first_pay_amount,new_pay_user_by_device)\n" +
                "SELECT * FROM (\n" +
                "    SELECT stat_day,cast(DATE_FORMAT(last_active_time, 'HH') AS bigint)  stat_hour,ad_uniqid,COALESCE(client,'unknown') client,monitor_id,campaign_id,ad_id,adcreative_id,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='log_in' THEN account_id END ) AS active_user,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='install_active'   AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd HH:mm') =DATE_FORMAT(first_time_active_time,'yyyy-MM-dd HH:mm') THEN distinct_id END ) AS new_active,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='register'         AND  match_state='1' THEN account_id END ) AS new_reg,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='register' THEN account_id END ) AS new_account,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='create_game_role' AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd HH') =DATE_FORMAT(first_create_game_role_time, 'yyyy-MM-dd HH')  THEN account_id  END ) AS new_create_role,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='create_game_role' THEN CONCAT(account_id,role_id)  END ) AS new_create_role_all,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='create_game_role' AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd HH') =DATE_FORMAT(first_create_game_role_time, 'yyyy-MM-dd HH')  AND stat_day  <=DATE_FORMAT(timestampadd(DAY, 180,TO_TIMESTAMP(register_time)), 'yyyy-MM-dd')  THEN account_id  END ) AS first_create_role,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='purchase'         AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd HH') =DATE_FORMAT(first_pay_time,'yyyy-MM-dd HH') THEN account_id END ) AS new_pay_user,\n" +
                "           COALESCE(SUM(CASE WHEN last_active_event='purchase'                    AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS new_pay_amount,\n" +
                "           COALESCE(SUM(CASE WHEN last_active_event='purchase' AND os='android'   AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS new_pay_amount_android,\n" +
                "           COALESCE(SUM(CASE WHEN last_active_event='purchase' AND os='ios'       AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS new_pay_amount_ios,\n" +
                "           COALESCE(SUM(CASE WHEN last_active_event='purchase'                    AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(first_pay_time,'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS first_pay_amount,\n" +
                "           COUNT(DISTINCT CASE WHEN last_active_event='purchase'         AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(first_pay_time,'yyyy-MM-dd') THEN account_id END ) AS new_pay_user_by_device\n" +
                "    FROM kafka_json_source_view\n" +
                "    WHERE COALESCE(ad_type,1) =1\n" +
                "    AND stat_day  >=DATE_FORMAT(timestampadd(DAY, -3, CURRENT_TIMESTAMP), 'yyyy-MM-dd')  --如果报错了 需要重头跑 尽量只更新最近三天的完整数据\n" +
                "    GROUP BY stat_day,cast(DATE_FORMAT(last_active_time, 'HH') AS bigint),ad_uniqid,COALESCE(client,'unknown'),monitor_id,campaign_id,ad_id,adcreative_id\n" +
                ")  tmp  WHERE  ad_uniqid IS NOT NULL  AND stat_day IS NOT NULL  AND  stat_hour IS NOT NULL  AND  client IS NOT NULL ");

        //创建sink2表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `realtime_overview_hour_after`(\n" +
                "  `stat_day` DATE NOT NULL COMMENT '统计日期',\n" +
                "  `stat_hour` BIGINT  NOT NULL COMMENT '统计小时',\n" +
                "  `ad_uniqid` STRING NOT NULL COMMENT '广告唯一id, 视小程序或app投放不同',\n" +
                "  `client` STRING NOT NULL COMMENT 'app或mp',\n" +
                "  `after_active_user` BIGINT NULL COMMENT '后项活跃用户数',\n" +
                "  `after_new_active` BIGINT  NULL COMMENT '后项新增激活',\n" +
                "  `after_new_reg` BIGINT  NULL COMMENT '后项新增注册',\n" +
                "  `after_new_account` BIGINT   NULL COMMENT '后项新增账号',\n" +
                "  `after_new_create_role` BIGINT   NULL COMMENT '后项新增创角',\n" +
                "  `after_new_create_role_all` BIGINT   NULL COMMENT '后项新增创角(全)',\n" +
                "  `after_new_pay_user` BIGINT   NULL COMMENT '后项新增付费用户数',\n" +
                "  `after_new_pay_amount` BIGINT   NULL COMMENT '后项新增充值',\n" +
                "  PRIMARY KEY (`stat_day`,`stat_hour`,`ad_uniqid`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://127.0.0.1:3306/ad',\n" +
                "   'driver'= 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'realtime_overview_hour',\n" +
                "   'username' = 'root',\n" +
                "   'password' = ''\n" +
                ")");

        //导入程序
        stmtSet.addInsertSql("INSERT INTO `realtime_overview_hour_after`(stat_day,stat_hour,ad_uniqid,client,after_active_user,after_new_active,after_new_reg,after_new_account,after_new_create_role,after_new_create_role_all,after_new_pay_user,after_new_pay_amount)\n" +
                "SELECT * FROM (\n" +
                "    SELECT first_time_day stat_day,stat_hour,ad_uniqid,COALESCE(client,'unknown') client,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='log_in'           THEN account_id END ) AS after_active_user,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='install_active'   AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd HH:mm') =DATE_FORMAT(first_time_active_time,'yyyy-MM-dd HH:mm') THEN distinct_id END ) AS after_new_active,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='register'         AND  match_state='1' THEN account_id END ) AS after_new_reg,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='register'         THEN account_id END ) AS after_new_account,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='create_game_role' THEN account_id  END ) AS after_new_create_role,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='create_game_role' THEN CONCAT(account_id,role_id)  END ) AS after_new_create_role_all,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='purchase'         THEN account_id END ) AS after_new_pay_user,\n" +
                "        COALESCE(SUM(CASE WHEN last_active_event='purchase'                    THEN last_pay_amount  END ),0) AS after_new_pay_amount\n" +
                "    FROM (  SELECT *,TO_DATE(DATE_FORMAT(first_time_active_time, 'yyyy-MM-dd'))  first_time_day,cast(DATE_FORMAT(first_time_active_time, 'HH') AS bigint)  stat_hour\n" +
                "            FROM kafka_json_source_view \n" +
                "            WHERE COALESCE(ad_type,1) =1 \n" +
                "                -- 事件时间 和 首次活跃时间 时间差小于24小时   \n" +
                "                AND UNIX_TIMESTAMP(last_active_time)-UNIX_TIMESTAMP(first_time_active_time) <= 24*3600\n" +
                "    ) tmp  WHERE 1=1   AND first_time_day >=DATE_FORMAT(timestampadd(DAY, -3, CURRENT_TIMESTAMP), 'yyyy-MM-dd')  --如果报错了 需要重头跑 尽量只更新最近三天的完整数据\n" +
                "    GROUP BY first_time_day,stat_hour,ad_uniqid,COALESCE(client,'unknown')\n" +
                ")  tmp  WHERE  ad_uniqid IS NOT NULL  AND stat_day IS NOT NULL  AND  stat_hour IS NOT NULL  AND  client IS NOT NULL ");

        // 执行刚刚添加的所有 INSERT 语句
        TableResult tableResult = stmtSet.execute();
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult.getJobClient().get().getJobStatus());

    }
}
