package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Random;


public class TaFormatEvent extends ScalarFunction {
    private Random random =  new Random();
    public String eval(String row, String AId, String DId,String timeKey, String event) throws Exception{
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);
            if (obj.getOrDefault(AId,"").equals("")) obj.remove(AId); else out.put("#account_id", obj.remove(AId));
            if (obj.getOrDefault(DId,"").equals("")) obj.remove(DId); else out.put("#distinct_id", obj.remove(DId));
            out.put("#type", "track");
            out.put("#time", obj.remove(timeKey));
            out.put("#event_name", obj.remove(event));
            out.put("properties", obj);
            if (!out.containsKey("#account_id") && !out.containsKey("#distinct_id")){
                out.put("#account_id", "system_"+String.valueOf(random.nextInt(10)));
            }
        } catch (Exception e){
            System.out.println("func: TaFormatEvent" + "; error data: " + row + "; error message :" + e.toString());
        }

        return  out.toString();
    }

    public String eval(String row, String AId, String DId,String timeKey, String event, String ipKey) throws Exception{
        JSONObject out = new JSONObject();
        try {
            JSONObject obj = JSONObject.parseObject(row);

            if (obj.getOrDefault(AId,"").equals("")) obj.remove(AId); else out.put("#account_id", obj.remove(AId));
            if (obj.getOrDefault(DId,"").equals("")) obj.remove(DId); else out.put("#distinct_id", obj.remove(DId));
            out.put("#type", "track");
            out.put("#time", obj.remove(timeKey));
            out.put("#ip", obj.remove(ipKey));
            out.put("#event_name", obj.remove(event));
            out.put("properties", obj);
            if (!out.containsKey("#account_id") && !out.containsKey("#distinct_id")){
                out.put("#account_id", "system_"+String.valueOf(random.nextInt(10)));
            }
        } catch (Exception e){
            System.out.println("func: TaFormatEvent" + "; error data: " + row + "; error message :" + e.toString());
        }
        return  out.toString();
    }
}
