package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Date;
import java.util.List;
import java.util.Map;


public class AdFormatEvent extends ScalarFunction {

    /**
     * 欢乐SDK上报格式转化
     * @param row
     * @param format_keys
     * @param removeKey
     * @param deviceKey
     * @return
     * @throws Exception
     */
    public String eval(String row, Map<String, String> format_keys, List<String> removeKey,List<String> deviceKey) throws Exception{
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);

            //移除不需要的key
            for (String e: removeKey){
                obj.remove(e);
            }

            for (Map.Entry<String, String> format_key : format_keys.entrySet()){
                if (obj.containsKey(format_key.getKey())){
                    out.put(format_key.getValue(), obj.remove(format_key.getKey()));
                }
            }

            //提取devicekey
            JSONObject device_properties = new JSONObject();
            for (String d : deviceKey){
                if (d.equals("real_ip")) {
                    device_properties.put("ip", obj.remove(d));
                } else if(d.equals("mac_address")){
                    if (obj.containsKey("mac_address")){
                        String mac = obj.getString(d);
                        if (!(mac.equals("02:00:00:00:00:00") || mac.equals("")) ){
                            device_properties.put("mac", mac);
                        }
                    }
                } else if (d.equals("oaid")){
                    if (obj.containsKey("oaid")){
                        String oaid = obj.getString(d);
                        if (!(oaid.equals("00000000-0000-0000-0000-000000000000") || oaid.equals(""))){
                            device_properties.put("oaid", oaid);
                        }
                    }
                } else {
                    device_properties.put(d, obj.get(d));
                }
            }

            out.put("device_properties",device_properties);
            out.put("event_properties", obj);
        } catch (Exception e){
            System.out.println("error message: "+ e.toString());
            System.out.println("error data: "+row);
        }
        return  out.toJSONString();
    }

    /**
     * ACT 小游戏转化使用
     * @param row
     * @param game_code
     * @param removeKey
     * @param rename_keys
     * @param format_keys
     * @param format_values
     * @return
     * @throws Exception
     */
    public String eval(String row, String game_code, List<String> removeKey, List<Tuple2<String, String>> rename_keys, List<String> format_keys, List<Tuple3<String, String, String>> format_values) throws Exception {
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            //设置游戏代码
            out.put("game_code", game_code);

            //移除不需要的key
            for (String e : removeKey) {
                obj.remove(e);
            }

            //rename_keys 重新命名
            for (Tuple2<String, String> rename_key : rename_keys){
                if (obj.containsKey(rename_key.f0)){
                    obj.put(rename_key.f1, obj.remove(rename_key.f0));
                }
            }

            //提取format_keys
            for (String format_key : format_keys){
                if (obj.containsKey(format_key)){
                    out.put(format_key, obj.remove(format_key));
                }
            }

            //字段值映射
            for (Tuple3<String,String,String> format_value: format_values){
                if (out.containsKey(format_value.f0) && out.getString(format_value.f0).equals(format_value.f1)){
                    out.put(format_value.f0, format_value.f2);
                }
            }

            out.put("event_properties", obj);

        } catch (Exception e){
            System.out.println("error message: "+ e.toString());
            System.out.println("error data: "+row);
        }
        return  out.toJSONString();
    }

    /**
     * 英雄杀小游戏上报广告格式转化
     * @param row
     * @param timeKey
     * @param zone
     * @param remove_keys
     * @param rename_keys
     * @param format_keys
     * @param format_values
     * @return
     * @throws Exception
     */

    public String eval(String row, String timeKey, Integer zone, List<String> remove_keys,List<Tuple2<String, String>> rename_keys, List<String> format_keys, List<Tuple3<String, String, String>> format_values) throws Exception {
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            //处理时间
            obj.put(timeKey, DateFormatUtils.format(new Date((Long) obj.remove(timeKey) - zone), "yyyy-MM-dd HH:mm:ss.SSS"));

            //移除不需要的key
            for (String e : remove_keys) {
                obj.remove(e);
            }

            //rename_keys 重新命名
            for (Tuple2<String, String> rename_key : rename_keys){
                if (obj.containsKey(rename_key.f0)){
                    obj.put(rename_key.f1, obj.remove(rename_key.f0));
                }
            }

            //提取format_keys
            for (String format_key : format_keys){
                if (obj.containsKey(format_key)){
                    out.put(format_key, obj.remove(format_key));
                }
            }

            //字段值映射
            for (Tuple3<String,String,String> format_value: format_values){
                if (out.containsKey(format_value.f0) && out.getString(format_value.f0).equals(format_value.f1)){
                    out.put(format_value.f0, format_value.f2);
                }
            }
            out.put("event_properties", obj);

        } catch (Exception e){
            System.out.println("func: AdFormatEvent" + "; error data: " + row + "; error message :" + e.toString());
        }
        return  out.toJSONString();
    }

}
