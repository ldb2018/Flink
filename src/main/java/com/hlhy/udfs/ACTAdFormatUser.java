package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;

public class ACTAdFormatUser extends ScalarFunction {


    public String eval(String row, String game_code, List<Tuple2<String, String>> format_keys, List<Tuple3<String, String, String>> extra_format_keys, List<Tuple3<String, String, String>> format_values) throws Exception{
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            if (obj.get("adInfo")!=null && obj.containsKey("adInfo") && !obj.getString("adInfo").equals("")){
                JSONObject query = obj.getJSONObject("adInfo");
                if (query.containsKey("otherInfo")){
                    query = query.getJSONObject("otherInfo");
                }
                boolean flag=true;

                //解析某些广告参数到外层,如果不存在就直接退出
                for (Tuple3<String,String,String> extra_format_key: extra_format_keys){
                    if (query.containsKey(extra_format_key.f0)){
                        if (extra_format_key.f2.equals("string")){
                            out.put(extra_format_key.f1, query.getString(extra_format_key.f0));
                        } else if (extra_format_key.f2.equals("integer")){
                            out.put(extra_format_key.f1, query.getInteger(extra_format_key.f0));
                        } else {
                            out.put(extra_format_key.f1, query.get(extra_format_key.f0));
                        }
                    } else {
                        out.clear();
                        flag=false;
                        break;
                    }
                }
                if (flag) {
                    out.put("game_code", game_code);
                    out.put("ad_properties", query.toJSONString());
                    out.put("ad_type", 1);
                    //字段名映射
                    for (Tuple2<String, String> format_key : format_keys) {
                        if (obj.containsKey(format_key.f0)) {
                            out.put(format_key.f1, obj.get(format_key.f0));
                        }
                    }

                    //字段值映射
                    for (Tuple3<String, String, String> format_value : format_values) {
                        if (out.containsKey(format_value.f0) && out.getString(format_value.f0).equals(format_value.f1)) {
                            out.put(format_value.f0, format_value.f2);
                        }
                    }

                    //判断是否是 register 事件
                    if (out.getString("last_active_event").equals("register")){
                        JSONObject account_infos = new JSONObject();
                        JSONObject account_info = new JSONObject();
                        account_info.put("ad_type",1);
                        account_info.put("register_time", out.getString("last_active_time"));
                        account_infos.put(out.getString("account_id"), account_info.toJSONString());

                        JSONObject state_properties = new JSONObject();
                        state_properties.put("first_time_active_time", out.getString("last_active_time"));
                        state_properties.put("match_state", 1);
                        state_properties.put("device_os", obj.getOrDefault("os", "unknown"));
                        out.put("state_properties", state_properties
                        );
                        out.put("account_infos", account_infos.toJSONString());
                    }
                    out.put("last_event_properties",row);
                }
            }
        } catch (Exception e){
            System.out.println("func: ACTAdFormatUser" + "; error data: " + row + "; error message :" + e.toString());
        }

        return out.toJSONString();
    }

}
