package com.hlhy.jobs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.hive.HiveModule;
import org.apache.hadoop.hive.conf.HiveConf;

import java.text.SimpleDateFormat;
import java.util.Date;


public class YxsKafka2HiveOds {

    // 定义函数逻辑
    public static class SetJsonObject extends ScalarFunction {
        public String eval(String data) {
            JSONObject object = JSON.parseObject(data);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String _time = object.getOrDefault("_time", "0").toString();
            String _time_str = sdf.format( new Date(Long.parseLong(_time)-8*3600*1000));
            object.put("_time",_time_str);

            String get_order_time = object.getOrDefault("get_order_time", "0").toString();
            String get_order_time_str = sdf.format( new Date(Long.parseLong(get_order_time)-8*3600*1000));
            object.put("get_order_time",get_order_time_str);

            String time_stamp = object.getOrDefault("time_stamp", "0").toString();
            String time_stamp_str = sdf.format( new Date(Long.parseLong(time_stamp)-8*3600*1000));
            object.put("time_stamp",time_stamp_str);

            return object.toJSONString();
        }
    }

    public static void main(String[] args) {
        //Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //开启CK 30分钟一次CP
        env.enableCheckpointing(1800000L, CheckpointingMode.EXACTLY_ONCE);
        // 检查点必须在5分钟内完成，或者被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(300000L);
        // 检查点运行出现多少次失败 默认检查点出错就结束任务  允许10个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        //3.设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://HLHY01/user/ldb/flink"));

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


//sink.rolling-policy.file-size、sink.rolling-policy.rollover-interval、checkpoint间隔，这三个选项，只要有一个条件达到了，然后就会触发分区文件的滚动，结束上一个文件的写入，生成新文件
        //tableEnv.registerFunction();
        //tableEnv.executeSql("DROP  TABLE IF EXISTS   yxs.ods_yxs_event_log_new ");
//        tableEnv.executeSql(
//                "CREATE EXTERNAL TABLE `yxs.ods_yxs_event_log_new`(                 "+
//                        "   `message` string,                                               "+
//                        "   `_accountid` string COMMENT '账户唯一id',                       "+
//                        "   `_time` string COMMENT '日志触发时间')                          "+
//                        " PARTITIONED BY (                                                  "+
//                        "   `date` string,                                                  "+
//                        "   `eventname` string)                                             "+
//                        " ROW FORMAT SERDE                                                  "+
//                        "   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'   "+
//                        " STORED AS INPUTFORMAT                                             "+
//                        "   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' "+
//                        " OUTPUTFORMAT                                                      "+
//                        "   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'"+
//                        " LOCATION                                                          "+
//                        "   'hdfs://HLHY01/data/yxs/ods/ods_yxs_event_log_new'              "+
//                        " TBLPROPERTIES (                                                   "+
//                        "   'bucketing_version'='2',                                        "+
//                        "   'last_modified_by'='hadoop',                                    "+
//                        "   'last_modified_time'='1644826629',                              "+
//                        "   'parquet.compression'='SNAPPY',                                 "+
//                        "   'partition.time-extractor.timestamp-pattern'='$date 00:00:00',  "+
//                        "   'sink.partition-commit.delay'='15min',                          "+
//                        "   'sink.partition-commit.policy.kind'='metastore,success-file',   "+
//                        "   'sink.partition-commit.trigger'='process-time',                 "+
//                        "   'sink.rolling-policy.check-interval'='1min',                    "+
//                        "   'sink.rolling-policy.file-size'='128M',                         "+
//                        "   'sink.rolling-policy.rollover-interval'='30min'                 "+
//                        "   )                                                               "
//        );

        tableEnv.createTemporarySystemFunction("set_json_object",SetJsonObject.class);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        //tableEnv.executeSql("DROP  TABLE IF EXISTS   ods_yxs_event_log_new_source");
        // 创建kafka_source表
        tableEnv.executeSql("CREATE TABLE  IF NOT EXISTS   ods_yxs_event_log_new_source ( \n" +
                "    log STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'properties.bootstrap.servers' = '10.16.0.10:9092',\n" +
                "    'topic' = 'yxs_event',\n" +
                "    'properties.group.id' = 'yxs_event_groupid',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'raw',\n" +
                "    'raw.charset' = 'UTF-8'\n" +
                ")");

        // 将数据导入hive
        tableEnv.executeSql("insert into  yxs.ods_yxs_event_log_new \n" +
                "select\n" +
                "set_json_object(log) message,\n" +
                "get_json_object(log,'$._accountid'), \n" +
                "from_unixtime(CAST (substr(get_json_object(log,'$._time'),0,10) AS bigint) ,'yyyy-MM-dd HH:mm:ss'), \n" +
                "from_unixtime(CAST (substr(get_json_object(log,'$._time'),0,10) AS bigint) ,'yyyy-MM-dd'), \n" +
                "get_json_object(log,'$._eventname') \n" +
                "from ods_yxs_event_log_new_source");
    }
}
