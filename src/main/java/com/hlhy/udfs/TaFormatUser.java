package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Date;
import java.util.List;

public class TaFormatUser extends ScalarFunction {

    /**
     * 英雄杀user上报到ta
     * @param row
     * @param AId
     * @param DId
     * @param typeKey
     * @return
     */
    public String eval(String row, String AId, String DId, String typeKey){
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            out.put("#account_id", obj.remove(AId));
            out.put("#distinct_id", obj.remove(DId));
            out.put("#type", obj.remove(typeKey));
            out.put("#time", DateFormatUtils.format(new Date(),"yyyy-MM-dd HH:mm:dd"));
            out.put("properties", obj.remove("value"));
        } catch (Exception e){
            System.out.println("error message: "+ e.toString());
            System.out.println("error data: "+row);
        }

        return  out.toString();
    }

    /**
     * ACT 数据上报到ta_user
     * @param row
     * @param AId
     * @param DId
     * @return
     */
    public String eval(String row, String AId, String DId, List<Tuple3<String, String, String>> cast_obj_values){
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            out.put("#account_id", obj.remove(AId));
            out.put("#distinct_id", obj.remove(DId));
            out.put("#type", "user_set");
            out.put("#time", DateFormatUtils.format(new Date(),"yyyy-MM-dd HH:mm:dd"));
            for (Tuple3<String,String,String> cast_obj_value: cast_obj_values){
                if (obj.containsKey(cast_obj_value.f0)){
                    if (cast_obj_value.f2.equals("json")){
                        obj.put(cast_obj_value.f1, JSONObject.parseObject(String.valueOf(obj.remove(cast_obj_value.f0))));
                    }
                }
            }
            out.put("properties", obj);
        } catch (Exception e){
            System.out.println("func: TaFormatUser" + "; error data: " + row + "; error message :" + e.toString());
        }
        return  out.toString();
    }
}
