import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

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
        env.setParallelism(1);
        //开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.createTemporarySystemFunction("set_json_object",SetJsonObject.class);

        tableEnv.executeSql("CREATE TABLE MyUserTableWithFilepath (\n" +
                "  logs STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:/C:/Users/dongbingli.INTRANET/Desktop/test20220711.txt',\n" +
                "  'format' = 'raw'\n" +
                ")");


        tableEnv.executeSql("CREATE TABLE Sink (\n" +
                "  logs STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        tableEnv.executeSql("insert into Sink select set_json_object(logs) from MyUserTableWithFilepath ");
        //tableEnv.executeSql(" select set_json_object(logs) from MyUserTableWithFilepath ").print();



    }
}
