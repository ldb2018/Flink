package com.hlhy.jobs;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.hadoop.hive.conf.HiveConf;


public class KafkaToHive {
    public static void main(String[] args) {
        //Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//
        //开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //3.设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));

        //Flink表环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 创建hive catalog，连接hive
        String name            = "myhive";
        String defaultDatabase = "flinkdb";
        String version         = "3.1.2";
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,"thrift://10.16.0.8:7004,thrift://10.16.0.16:7004");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,"hdfs://HLHY01/usr/hive/warehouse");
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConf, version);
        // 注册catalog
        tableEnv.registerCatalog("myhive", hive);

        // 设置当前使用catalog，相当于切换数据库的操作
        tableEnv.useCatalog("myhive");


        // 设置可以使用hive的内置函数
        tableEnv.loadModule(name, new HiveModule(version));

        //指定方言 用来创建hive表
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("DROP  TABLE IF EXISTS   ad.ods_adjust_click");
        tableEnv.executeSql(
                "CREATE  EXTERNAL TABLE IF NOT EXISTS   ad.ods_adjust_click("+
                        "  info string COMMENT 'json点击信息')"+
                        "PARTITIONED BY ( "+
                        "  `dt` string COMMENT '日期')"+
                        "ROW FORMAT SERDE "+
                        "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "+
                        "STORED AS INPUTFORMAT "+
                        "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "+
                        "OUTPUTFORMAT "+
                        "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"+
                        "LOCATION"+
                        "  'hdfs://HLHY01/data/ad/ods/ods_adjust_click'"+
                        "TBLPROPERTIES ("+
                        "'parquet.compression'='SNAPPY', "+
                        "'partition.time-extractor.timestamp-pattern'='$dt 00:00:00',"+
                        "'sink.partition-commit.trigger'='partition-time',"+
                        "'sink.partition-commit.delay'='1min',"+
                        "'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',"+
                        "'sink.partition-commit.policy.kind'='metastore,success-file',"+
                        "'sink.rolling-policy.file-size' = '128M',"+
                        "'sink.rolling-policy.check-interval'='1min',"+
                        "'sink.rolling-policy.rollover-interval' = '30min'"+
                        "  )"
        );

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("DROP  TABLE IF EXISTS   kafka_source_table");
        // 创建kafka_source表
        tableEnv.executeSql("CREATE TABLE kafka_source_table ( \n" +
                "    log STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'properties.bootstrap.servers' = '10.16.0.10:9092',\n" +
                "    'topic' = 'ad_callback_ou',\n" +
                "    'properties.group.id' = 'ad_adjust_consumer6',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'raw',\n" +
                "    'raw.charset' = 'UTF-8'\n" +
                ")");

        // 将数据导入hive
        tableEnv.executeSql("insert into  ad.ods_adjust_click \n" +
                "select\n" +
                "log,\n" +
                "from_unixtime(CAST (substr(get_json_object(log,'$.created_at'),0,10) AS bigint) ,'yyyy-MM-dd') \n" +
                "from kafka_source_table");
    }
}
