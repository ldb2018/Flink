package com.hlhy.udfs;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class ParseTaEvent  extends ScalarFunction {
    public ParseTaEvent() {
    }
//, String eventKey, List<String> events
    public String eval(String row) throws Exception {
        

        JSONObject out = new JSONObject();

        try {
            //1.将json字符串转化为json对象
            JSONObject json = JSONObject.parseObject(row);
            String appid = (String)json.get("appid");
            JSONObject data_object = (JSONObject)json.get("data_object");

            //2.取出data
            JSONArray data = data_object.getJSONArray("data");

            //3.取出data中的数组
            JSONObject obj = data.getJSONObject(0);

            //4.取出详情值
            String event_name=(String)obj.get("#event_name");
            String account_id=(String)obj.get("#account_id");
            String distinct_id=(String)obj.get("#distinct_id");
            String type=(String)obj.get("#type");
            String record_time=(String)obj.get("#time");
            JSONObject event_properties=obj.getJSONObject("properties");
            out.put("game_code", "ro");
            out.put("event_name", event_name);
            out.put("record_time", record_time);
            out.put("account_id", account_id);
            out.put("distinct_id", distinct_id);
            out.put("event_properties", event_properties.toString());
            out.put("device_properties", "");
            out.put("package_id", "");
        } catch (Exception e) {
            System.out.println("error message: " + e.toString());
            System.out.println("error data: " + row);
        }

        return out.toString();
    }

    public static void main(String[] args) throws Exception {
        ParseTaEvent ta= new ParseTaEvent();
        String s ="{\"appid\":\"44b2f954adb343a0b80b8ebcf204eee8\",\"client_ip\":\"\",\"data_object\":{\"data\":[{\"#account_id\":\"506925\",\"#time\":\"2022-07-20 15:02:25.000\",\"#distinct_id\":\"BqaN8kdpacboEHNrblMQGg\",\"#uuid\":\"1d058c6e-b016-875d-561e-c3998f1a10e0\",\"#event_name\":\"assistantflow\",\"#type\":\"track\",\"properties\":{\"createtime\":\"2022-05-07 18:22:43.000\",\"unionid\":0,\"gender\":0,\"vrolename\":\"MarthaAvignon\",\"level\":68,\"Reason\":2,\"ijoblevel\":88,\"jobid\":3101,\"playerfriendsnum\":1,\"platid\":1,\"vgameappid\":\"1106940687\",\"#jobid\":\"20220721193715868\",\"izoneareaid\":1,\"rolevip\":0,\"recommendsystemid\":[\"[]\"],\"regchannel\":0,\"selectsystemid\":0,\"chargegold\":0,\"unionname\":\"\",\"loginchannel\":1,\"OperateType\":0,\"gamesvrid\":\"2\",\"assistantprogress\":[{\"num\":0.0,\"id\":\"20007\"},{\"num\":0.0,\"id\":\"3095\"},{\"num\":0.0,\"id\":\"3094\"},{\"num\":0.0,\"id\":\"20007\"},{\"num\":0.0,\"id\":\"10821\"},{\"num\":0.0,\"id\":\"115\"},{\"num\":0.93952802359882,\"id\":\"143\"},{\"num\":0.0,\"id\":\"20005\"},{\"num\":0.0,\"id\":\"10802\"},{\"num\":0.0,\"id\":\"20003\"},{\"num\":0.0,\"id\":\"9009\"},{\"num\":0.0,\"id\":\"20004\"},{\"num\":0.0,\"id\":\"20004\"},{\"num\":0.92,\"id\":\"9006\"},{\"num\":0.0,\"id\":\"10804\"},{\"num\":1.0,\"id\":\"102\"}]}}]},\"data_source\":\"Import_Tools\",\"receive_time\":\"2022-07-21 19:38:03.405\"}\n";
        System.out.println(ta.eval(s));
    }

}

