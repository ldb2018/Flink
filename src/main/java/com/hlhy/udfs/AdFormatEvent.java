package com.hlhy.udfs;
import com.alibaba.fastjson.JSONObject;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class AdFormatEvent extends ScalarFunction {
    public AdFormatEvent() {
    }

    public String eval(String row, String game_code, String Aid, String event, String timeKey, List<String> removeKey, List<String> deviceKey) throws Exception {
        JSONObject out = new JSONObject();

        try {
            JSONObject obj = JSONObject.parseObject(row);
            Iterator var10 = removeKey.iterator();

            while(var10.hasNext()) {
                String e = (String)var10.next();
                obj.remove(e);
            }

            JSONObject device_properties = new JSONObject();
            Iterator var15 = deviceKey.iterator();

            while(var15.hasNext()) {
                String d = (String)var15.next();
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

    public String eval(String row, String game_code, String Aid, String event, String timeKey, List<String> removeKey, List<String> deviceKey, Integer zone) throws Exception {
        JSONObject out = new JSONObject();

        try {
            JSONObject obj = JSONObject.parseObject(row);
            Iterator var11 = removeKey.iterator();

            while(var11.hasNext()) {
                String e = (String)var11.next();
                obj.remove(e);
            }

            JSONObject device_properties = new JSONObject();
            Iterator var16 = deviceKey.iterator();

            while(var16.hasNext()) {
                String d = (String)var16.next();
                device_properties.put(d, obj.remove(d));
            }

            out.put("game_code", game_code);
            out.put("record_time", DateFormatUtils.format(new Date((Long)obj.remove(timeKey) - (long)zone), "yyyy-MM-dd HH:mm:ss.SSS"));
            out.put("event_name", obj.remove(event));
            out.put("account_id", obj.remove(Aid));
            out.put("device_properties", device_properties);
            out.put("event_properties", obj);
        } catch (Exception var14) {
            System.out.println("error message: " + var14.toString());
            System.out.println("error data: " + row);
        }

        return out.toJSONString();
    }

    public String eval(String row, Map<String, String> format_keys, List<String> removeKey, List<String> deviceKey) throws Exception {
        JSONObject out = new JSONObject();

        try {
            JSONObject obj = JSONObject.parseObject(row);
            Iterator var7 = removeKey.iterator();

            while(var7.hasNext()) {
                String e = (String)var7.next();
                obj.remove(e);
            }

            var7 = format_keys.entrySet().iterator();

            while(var7.hasNext()) {
                Entry<String, String> format_key = (Entry)var7.next();
                if (obj.containsKey(format_key.getKey())) {
                    out.put((String)format_key.getValue(), obj.get(format_key.getKey()));
                }
            }

            JSONObject device_properties = new JSONObject();
            Iterator var13 = deviceKey.iterator();

            while(var13.hasNext()) {
                String d = (String)var13.next();
                device_properties.put(d, obj.remove(d));
            }

            out.put("device_properties", device_properties);
            out.put("event_properties", obj);
        } catch (Exception var10) {
            System.out.println("error message: " + var10.toString());
            System.out.println("error data: " + row);
        }

        return out.toJSONString();
    }
}
