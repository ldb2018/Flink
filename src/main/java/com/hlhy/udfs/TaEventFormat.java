package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.functions.ScalarFunction;

public class TaEventFormat extends ScalarFunction {
    public TaEventFormat() {
    }

    public String eval(String row, String game_code, String Aid, String event, String timeKey, List<String> removeKey, List<String> deviceKey) throws Exception {
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            Iterator var10 = removeKey.iterator();

            while (var10.hasNext()) {
                String e = (String) var10.next();
                obj.remove(e);
            }

            JSONObject device_properties = new JSONObject();
            Iterator var15 = deviceKey.iterator();

            while (var15.hasNext()) {
                String d = (String) var15.next();
                device_properties.put(d, obj.remove(d));
            }

            out.put("game_code", game_code);
            out.put("record_time", obj.remove(timeKey));
            out.put("event_name", obj.remove(event));
            out.put("account_id", obj.remove(Aid));
            out.put("device_properties", device_properties);
            out.put("event_properties", obj);
        } catch (Exception var13) {
            System.out.println("error message: " + var13.toString());
            System.out.println("error data: " + row);
        }

        return out.toJSONString();
    }
}