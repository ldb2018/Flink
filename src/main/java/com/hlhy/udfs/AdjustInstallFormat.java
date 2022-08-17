package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AdjustInstallFormat extends ScalarFunction {
    public AdjustInstallFormat() {
    }
//insert into tc_sdk_out select AdFormatEvent(log, MAP['game_code','game_code','eventname','event_name','record_time','record_time','account_id','account_id','distinct_id','distinct_id','ad_channel','package_id'], array['env'], array['android_id','oaid','device_ua','real_ip','mac_address'] ) from tc_sdk_in WHERE isExistsEvent(log,'eventname',array['install_active','register','log_in','create_game_role','purchase'])=true
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
                Map.Entry<String, String> format_key = (Map.Entry)var7.next();
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
