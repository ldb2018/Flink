package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;

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

    public static void main(String[] args) {
        MapToJson mtj =new MapToJson();
        Map map = new HashMap();
        map.put("name","ldb");
        map.put("age","11");
        System.out.println(mtj.eval(map));;
    }
}