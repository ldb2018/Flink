package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;


public class YxsAdFormatUser extends ScalarFunction {

    public String eval(String row, String game_code, String Aid, List<Tuple3<String, String, String>> extra_format_keys) throws Exception{
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            JSONObject values = obj.getJSONObject("value");
            if (values.containsKey("options") && !values.getString("options").equals("") ){
                JSONObject query = values.getJSONObject("options").getJSONObject("query");

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
                    out.put("account_id", obj.get(Aid));
                    out.put("ad_properties", query);
                    out.put("ad_type", 1);
                }
            }
        } catch (Exception e){
            System.out.println("func: YxsAdFormatUser" + "; error data: " + row + "; error message :" + e.toString());
        }

        return out.toJSONString();
    }

}
