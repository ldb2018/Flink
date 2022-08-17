package com.hlhy.jobs;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.module.hive.HiveModule;

import java.util.Map;


public class ParseTaEvent {

    // 定义函数逻辑
    public  class MapToJson extends ScalarFunction {
        public String eval(Map<String, String> map) {

            JSONObject out = new JSONObject();

            try {
                for (String key : map.keySet()) {
                    System.out.println("key= "+ key + " and value= " + map.get(key));
                    out.put(key,map.get(key));
                }
            } catch (Exception e) {
                System.out.println("error message: " + e.toString());
                System.out.println("error data: " + map.toString());
            }

            return out.toString();
        }
    }
    public static void main(String[] args) {
        //Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //开启CK 1分钟一次CP
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
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
        String version         = "3.1.2";
        tableEnv.loadModule(name, new HiveModule(version));
        tableEnv.createTemporarySystemFunction("maptojson", MapToJson.class);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        //建source表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `test_ta_event`(\n" +
                "  `appid` STRING COMMENT 'appid',\n" +
                "  `client_ip` STRING COMMENT 'client_ip',\n" +
                "  `data_object` STRING COMMENT 'data_object',\n" +
                "  `data_source` STRING COMMENT 'data_source',\n" +
                "  `receive_time` STRING COMMENT 'receive_time'\n" +
                "\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'properties.bootstrap.servers' = 'emr_kafka1:9092,emr_kafka2:9092,emr_kafka3:9092',\n" +
                "  'topic' = 'test_ta_event',\n" +
                "  'properties.group.id' = 'test_ta_event_groupid',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ");\n" +
                "");

        // 建sink表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  test_ta_event_format(\n" +
                "  log STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "      'topic' = 'test_ta_event_format',\n" +
                "      'properties.bootstrap.servers' = 'emr_kafka1:9092,emr_kafka2:9092,emr_kafka3:9092',\n" +
                "      'properties.group.id' = 'test_ta_event_format-out',\n" +
                "      'format' = 'raw'\n" +
                ");");

        //插入数据
        tableEnv.executeSql("insert into  test_ta_event_format\n" +
                "select maptojson(MAP['game_code',game_code,'event_name',event_name,'record_time',record_time,'account_id',account_id,'distinct_id',distinct_id,'event_properties',event_properties,'device_properties',device_properties,'package_id',package_id]) from (\n" +
                "    select 'ro'  as game_code,\n" +
                "        get_json_object(data,'$.event_name')  event_name,\n" +
                "        get_json_object(data,'$.time') record_time,\n" +
                "        get_json_object(data,'$.account_id')  account_id,\n" +
                "        get_json_object(data,'$.distinct_id') distinct_id,\n" +
                "        get_json_object(data,'$.properties')  event_properties,\n" +
                "        '' device_properties ,\n" +
                "        '' package_id\n" +
                "    from (\n" +
                "        select regexp_replace(get_json_object(data_object,'$.data[0]'),'#','') as data\n" +
                "        from test_ta_event \n" +
                "        where appid='44b2f954adb343a0b80b8ebcf204eee8'\n" +
                "    ) tmp \n" +
                ") tmp \n" +
                ";");


    }
}
