package com.hlhy.udfs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.flink.table.functions.ScalarFunction;

public class isExistsEvent extends ScalarFunction {
    public isExistsEvent() {
    }

    public Boolean eval(String row, String filterappid, List<String> filterevents) throws Exception {
        boolean flag = false;

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

            if (filterevents.contains(event_name)&&filterappid.equals(appid)){
                flag = true;
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }

        return flag;
    }
}