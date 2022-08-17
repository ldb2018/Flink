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


public class AdMatchMiniReport {
    // 定义函数逻辑
    public static class GetInstallActiveTime extends ScalarFunction {

        public Connection conn;

        //只处理 激活事件  同时确保是首次激活
        public String eval(String game_code, String distinct_id, String last_active_time, String jsonstr) {
            return eval(game_code, distinct_id, last_active_time, jsonstr, "device_install_active_info");
        }

        public String eval(String game_code, String distinct_id, String last_active_time, String jsonstr, String tablename) {
            String res = "1970-01-01 00:00:00";
            String message = game_code + "\t" + distinct_id + "\t" + last_active_time;
            if (StringUtils.isEmpty(game_code) || StringUtils.isEmpty(distinct_id) || StringUtils.isEmpty(distinct_id)) {
                return res;
            }
            try {
                conn = DriverManager.getConnection("jdbc:mysql://172.17.0.22:3306/ad?characterEncoding=utf-8", "root", "Bigdata.2021!@#");
                //starRocksConn = DriverManager.getConnection("jdbc:mysql://10.16.0.8:9030/ad?characterEncoding=utf-8", "root", "root123");

                String sql = "SELECT * FROM " + tablename + " WHERE game_code=? and distinct_id=?";
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                preparedStatement.setString(1, game_code);
                preparedStatement.setString(2, distinct_id);
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {//只取一个
                    res = resultSet.getString("last_active_time");
                    System.out.println("已经有数据\t" + message + "\t" + res);
                } else {
                    String updatesql = "insert into " + tablename + "(game_code,distinct_id,last_active_time,jsonstr) values(?,?,?,?)";
                    //数据库没有数据  插入一条记录
                    preparedStatement = conn.prepareStatement(updatesql);
                    preparedStatement.setString(1, game_code);
                    preparedStatement.setString(2, distinct_id);
                    preparedStatement.setString(3, last_active_time);
                    preparedStatement.setString(4, jsonstr);
                    int isOk = preparedStatement.executeUpdate();
                    if (isOk > 0) {
                        System.out.println("插入成功\t" + message);
                    } else {
                        System.out.println("插入失败\t" + message);
                    }
                    res = last_active_time;
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("执行sql异常: " + e.toString());
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.out.println("关闭conn异常: " + e.toString());
                }
            }
            return res;
        }
    }


    public static void main(String[] args) {
        //Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启CK
        env.enableCheckpointing(300000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(300000L);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://HLHY01/user/ldb/flink");
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        //设置状态后端为hashmap
        env.setStateBackend(new HashMapStateBackend());

        //关闭全局任务链 否则只能提交一个sql
        //env.disableOperatorChaining();

        //Flink表环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //注册udf函数
        tableEnv.createTemporarySystemFunction("GetInstallActiveTime", GetInstallActiveTime.class);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        StreamStatementSet stmtSet  = tableEnv.createStatementSet();

        //第一步建source表 -小游戏
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `ad_user_match_mini`(\n" +
                "  `game_code` STRING COMMENT '主游戏',\n" +
                "  `distinct_id` STRING COMMENT '设备唯一id',\n" +
                "  `account_id` STRING COMMENT '游戏账号',\n" +
                "  `account_infos` STRING COMMENT '该设备登录过的账号所有信息',\n" +
                "  `ad_type` BIGINT COMMENT '判断当前账号类型 1 广告新增；2 广告回流； 3 自然量  目前统计的只有 广告新增',\n" +
                "  `ad_plat` STRING COMMENT '媒体平台',\n" +
                "  `ad_time` STRING COMMENT '广告回传时间',\n" +
                "  `ad_properties` STRING COMMENT '广告回传参数',\n" +
                "  `last_active_time` STRING COMMENT '设备最后一次活跃时间',\n" +
                "  `last_active_event` STRING COMMENT '设备最后一次活跃事件  枚举 install_active  register log_in',\n" +
                "  `device_properties` STRING COMMENT '设备信息json',\n" +
                "  `last_event_properties` STRING COMMENT '当前设备用户行为信息json',\n" +
                "  `state_properties` STRING COMMENT '设备状态信息json',\n" +
                "  `monitor_id` BIGINT COMMENT '监测序号id',\n" +
                "  `package_id` STRING COMMENT '包id'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'emr_kafka1:9092,emr_kafka2:9092,emr_kafka3:9092',\n" +
                "  'topic' = 'mini_ad_match_result',\n" +
                "  'properties.group.id' = 'mini_game_callback',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  --'scan.startup.mode' = 'latest-offset',\n" +
                "  --'scan.startup.mode' = 'timestamp',\n" +
                "  --'scan.startup.timestamp-millis' = '1658386200000',\n" +
                "  --'scan.startup.mode' = 'specific-offsets',\n" +
                "  --'scan.startup.specific-offsets' = 'partition:0,offset:52;partition:1,offset:52',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")");

        //第二步创建临时视图  统一格式
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
                "    'properties.group.id' = 'ad_match_result_report_view_groupid',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'csv',\n" +
                "    'csv.field-delimiter'='\\t',\n" +
                "    'csv.ignore-parse-errors'='true',\n" +
                "    'csv.disable-quote-character' ='true'\n" +
                ")");
        stmtSet.addInsertSql("INSERT INTO kafka_json_source_view\n" +
                "SELECT stat_day,CASE WHEN ad_uniqid IN ('0') THEN CONCAT(CAST (monitor_id AS STRING),'_unknown') ELSE  ad_uniqid END  ad_uniqid,client,os,game_code,distinct_id,account_id,ad_type,ad_plat,ad_time,campaign_id,ad_id,adcreative_id,last_active_time,last_active_event,first_time_active_time,match_state,monitor_id,package_id,register_time,first_pay_time,last_pay_amount,role_id,first_create_game_role_time FROM (\n" +
                "     SELECT \n" +
                "            TO_DATE(DATE_FORMAT(last_active_time, 'yyyy-MM-dd')) stat_day ,\n" +
                "            CASE WHEN ad_plat='2'  THEN SPLIT_INDEX(JSON_VALUE(ad_properties,'$.weixinadinfo'),'.', 0)  ELSE  JSON_VALUE(ad_properties,'$.creative_id') END   ad_uniqid,--微信weixinadinfo 巨量 creative_id \n" +
                "            'mp' client,\n" +
                "            CASE WHEN  LOWER(JSON_VALUE(state_properties,'$.device_os'))  LIKE '%android%'  THEN 'android' WHEN  LOWER(JSON_VALUE(state_properties,'$.device_os'))  LIKE '%ios%' THEN 'ios'  ELSE  'unknown' END os,--目前改成是从state_properties 提取\n" +
                "            game_code,\n" +
                "            distinct_id,\n" +
                "            account_id,\n" +
                "            ad_type,\n" +
                "            ad_plat,\n" +
                "            ad_time,\n" +
                "            JSON_VALUE(ad_properties,'$.campaign_id') campaign_id,\n" +
                "            CASE WHEN ad_plat='2'  THEN SPLIT_INDEX(JSON_VALUE(ad_properties,'$.weixinadinfo'),'.', 0)  ELSE  JSON_VALUE(ad_properties,'$.ad_id') END    ad_id,\n" +
                "            JSON_VALUE(ad_properties,'$.creative_id') adcreative_id,\n" +
                "            last_active_time,\n" +
                "            last_active_event,\n" +
                "            CASE WHEN last_active_event='install_active'  THEN GetInstallActiveTime(game_code,distinct_id,last_active_time,'')  ELSE  JSON_VALUE(state_properties,'$.first_time_active_time') END first_time_active_time,--小游戏的激活需要去重时间\n" +
                "            JSON_VALUE(state_properties,'$.match_state') match_state,\n" +
                "            monitor_id,\n" +
                "            package_id,\n" +
                "            JSON_VALUE(JSON_VALUE(account_infos,CONCAT('$.',account_id)),'$.register_time') register_time,\n" +
                "            JSON_VALUE(JSON_VALUE(account_infos,CONCAT('$.',account_id)),'$.first_pay_time') first_pay_time,\n" +
                "            COALESCE(cast (cast(JSON_VALUE(JSON_VALUE(account_infos,CONCAT('$.',account_id)),'$.last_pay_amount') AS double)*100  AS BIGINT),0) last_pay_amount,\n" +
                "            JSON_VALUE(last_event_properties,'$.role_id') role_id,\n" +
                "            JSON_VALUE(state_properties,'$.first_create_game_role_time') first_create_game_role_time\n" +
                "       FROM `ad_user_match_mini`\n" +
                "      \n" +
                ")  tmp  \n" +
                " WHERE ad_plat <> ''  AND  last_active_event IS NOT NULL  AND  last_active_time IS NOT NULL\n" +
                "");

        //第三步建立sink表 用来导入小同要的结果表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `overview`(\n" +
                "  `stat_day` DATE NOT NULL COMMENT '统计日期',\n" +
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
                "  `new_pay_user` BIGINT   NULL COMMENT '新增付费用户数',\n" +
                "  `new_pay_amount` BIGINT   NULL COMMENT '新增充值',\n" +
                "  `new_pay_amount_android` BIGINT   NULL COMMENT '新增充值-安卓',\n" +
                "  `new_pay_amount_ios` BIGINT   NULL COMMENT '新增充值-IOS',\n" +
                "  `first_pay_amount` BIGINT   NULL COMMENT '单设备新增付费账号新增充值 不管注册是哪天 只看是不是今天首充付费',\n" +
                "  `new_pay_user_by_device` BIGINT   NULL COMMENT '单设备新增付费账号 不管注册是哪天 只看是不是今天首充付费',\n" +
                "  PRIMARY KEY (`stat_day`,`ad_uniqid`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://172.17.32.38:3306/ad',\n" +
                "   'table-name' = 'overview',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'yS3Mf4duDKJg',\n" +
                "   'sink.buffer-flush.max-rows'='100',\n" +
                "   'sink.buffer-flush.interval'='5s'\n" +
                ")");
        stmtSet.addInsertSql("INSERT INTO `overview`(stat_day,ad_uniqid,client,monitor_id,campaign_id,ad_id,adcreative_id,active_user,new_active,new_reg,new_account,new_create_role,new_create_role_all,new_pay_user,new_pay_amount,new_pay_amount_android,new_pay_amount_ios,first_pay_amount,new_pay_user_by_device)\n" +
                "SELECT * FROM (\n" +
                "    SELECT stat_day,ad_uniqid,COALESCE(client,'unknown') client,monitor_id,campaign_id,ad_id,adcreative_id,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='log_in' THEN account_id END ) AS active_user,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='install_active'   AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(first_time_active_time,'yyyy-MM-dd') THEN distinct_id END ) AS new_active,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='register'         AND  match_state='1' THEN account_id END ) AS new_reg,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='register' THEN account_id END ) AS new_account,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='create_game_role' AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN account_id  END ) AS new_create_role,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='create_game_role' THEN CONCAT(account_id,role_id)  END ) AS new_create_role_all,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='purchase'         AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN account_id END ) AS new_pay_user,\n" +
                "        COALESCE(SUM(CASE WHEN last_active_event='purchase'                    AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS new_pay_amount,\n" +
                "        COALESCE(SUM(CASE WHEN last_active_event='purchase' AND os='android'   AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS new_pay_amount_android,\n" +
                "        COALESCE(SUM(CASE WHEN last_active_event='purchase' AND os='ios'       AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(register_time, 'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS new_pay_amount_ios,\n" +
                "        COALESCE(SUM(CASE WHEN last_active_event='purchase'                    AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(first_pay_time,'yyyy-MM-dd') THEN last_pay_amount  END ),0) AS first_pay_amount,\n" +
                "        COUNT(DISTINCT CASE WHEN last_active_event='purchase'         AND  DATE_FORMAT(last_active_time, 'yyyy-MM-dd') =DATE_FORMAT(first_pay_time,'yyyy-MM-dd') THEN account_id END ) AS new_pay_user_by_device\n" +
                "    FROM kafka_json_source_view\n" +
                "    WHERE COALESCE(ad_type,1) =1  --是否只统计广告新增 所有统计的事件中都带了 ad_type这个字段\n" +
                "    AND  stat_day  >=DATE_FORMAT(timestampadd(HOUR, -120, CURRENT_TIMESTAMP), 'yyyy-MM-dd')  --如果报错了 需要重头跑 尽量只更新最近三天的完整数据\n" +
                "    GROUP BY stat_day,ad_uniqid,COALESCE(client,'unknown'),monitor_id,campaign_id,ad_id,adcreative_id\n" +
                ")  tmp  WHERE  ad_uniqid IS NOT NULL  AND stat_day IS NOT NULL  AND  client IS NOT NULL \n" +
                "");


        // 执行刚刚添加的所有 INSERT 语句
        TableResult tableResult = stmtSet.execute();
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult.getJobClient().get().getJobStatus());

    }
}
