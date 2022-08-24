//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//
package com.hlhy.udfs;
import com.alibaba.fastjson.JSONObject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.ScalarFunction;

public class ACTAdFormatUser extends ScalarFunction {
    public ACTAdFormatUser() {
    }

    public String eval(String row, String game_code, Map<String, String> format_keys, List<Tuple3<String, String, String>> extra_format_keys, List<Tuple3<String, String, String>> format_values) throws Exception {
        JSONObject out = new JSONObject();

        try {
            JSONObject obj = JSONObject.parseObject(row);
            if (obj.get("adInfo") != null && obj.containsKey("adInfo") && !obj.getString("adInfo").equals("")) {
                JSONObject query = obj.getJSONObject("adInfo");
                if (query.containsKey("otherInfo")) {
                    query = query.getJSONObject("otherInfo");
                }

                boolean flag = true;
                Iterator var10 = extra_format_keys.iterator();

                Tuple3 format_value;
                while(var10.hasNext()) {
                    format_value = (Tuple3)var10.next();
                    if (!query.containsKey(format_value.f0)) {
                        out.clear();
                        flag = false;
                        break;
                    }

                    if (((String)format_value.f2).equals("string")) {
                        out.put((String)format_value.f1, query.getString((String)format_value.f0));
                    } else if (((String)format_value.f2).equals("integer")) {
                        out.put((String)format_value.f1, query.getInteger((String)format_value.f0));
                    } else {
                        out.put((String)format_value.f1, query.get(format_value.f0));
                    }
                }

                if (flag) {
                    out.put("game_code", game_code);
                    out.put("ad_properties", query.toJSONString());
                    out.put("ad_type", 1);
                    var10 = format_keys.entrySet().iterator();

                    while(var10.hasNext()) {
                        Entry<String, String> format_key = (Entry)var10.next();
                        if (obj.containsKey(format_key.getKey())) {
                            out.put((String)format_key.getValue(), obj.get(format_key.getKey()));
                        }
                    }

                    var10 = format_values.iterator();

                    while(var10.hasNext()) {
                        format_value = (Tuple3)var10.next();
                        if (out.containsKey(format_value.f0) && out.getString((String)format_value.f0).equals(format_value.f1)) {
                            out.put((String)format_value.f0, format_value.f2);
                        }
                    }

                    if (out.getString("last_active_event").equals("register")) {
                        JSONObject account_infos = new JSONObject();
                        JSONObject account_info = new JSONObject();
                        account_info.put("ad_type", 1);
                        account_info.put("register_time", out.getString("last_active_time"));
                        account_infos.put(out.getString("account_id"), account_info.toJSONString());
                        JSONObject state_properties = new JSONObject();
                        state_properties.put("first_time_active_time", out.getString("last_active_time"));
                        state_properties.put("match_state", 1);
                        state_properties.put("device_os", obj.getOrDefault("os", "unknown"));
                        out.put("state_properties", state_properties.toJSONString());
                        out.put("account_infos", account_infos.toJSONString());
                    }

                    out.put("last_event_properties", row);
                }
            }
        } catch (Exception var13) {
            System.out.println("error data: " + row + "; error message :" + var13.toString());
        }

        return out.toJSONString();
    }
}
