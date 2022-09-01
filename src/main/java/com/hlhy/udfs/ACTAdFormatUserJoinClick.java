package com.hlhy.udfs;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.*;

public class ACTAdFormatUserJoinClick extends ScalarFunction {
    public ACTAdFormatUserJoinClick() {
    }

    public Connection conn;

    public String eval(String row, String ad_plat) {
        //只查 腾讯的
        if ("2".equals(ad_plat)) {
            return eval(row);
        }
        return row;
    }

    public String eval(String row) {

        try {
            JSONObject obj = JSONObject.parseObject(row);
            conn = DriverManager.getConnection("jdbc:mysql://172.17.0.22:3306/ad?characterEncoding=utf-8", "root", "Bigdata.2021!@#");
            //conn = DriverManager.getConnection("jdbc:mysql://127.0.01:3306/ad?characterEncoding=utf-8", "root", "");
            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;
            String sql = "";

            //可以顺带把 first_time_install_active_time
            String last_active_event = obj.getString("last_active_event");
            JSONObject state_properties = obj.getJSONObject("state_properties");
            String last_active_time = obj.getString("last_active_time");
            String first_time_install_active_time = last_active_time;
            if ("install_active".equals(last_active_event)) {
                String game_code = obj.getString("game_code");
                String distinct_id = obj.getString("distinct_id");
                String jsonstr = "";
                sql = "SELECT * FROM ad.device_install_active_info WHERE game_code=? and distinct_id=? ";
                preparedStatement = conn.prepareStatement(sql);
                preparedStatement.setString(1, game_code);
                preparedStatement.setString(2, distinct_id);
                resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {//只取一个
                    first_time_install_active_time = resultSet.getString("last_active_time");
                } else {
                    sql = "insert into ad.device_install_active_info(game_code,distinct_id,last_active_time,jsonstr) values(?,?,?,?)";
                    //数据库没有数据  插入一条记录
                    preparedStatement = conn.prepareStatement(sql);
                    preparedStatement.setString(1, game_code);
                    preparedStatement.setString(2, distinct_id);
                    preparedStatement.setString(3, last_active_time);
                    preparedStatement.setString(4, jsonstr);
                    preparedStatement.executeUpdate();
                }
                state_properties.put("first_time_install_active_time", first_time_install_active_time);
                obj.put("state_properties", state_properties.toJSONString());
            }

            //如果是 腾讯需要再匹配 ad_id 和 callback
            String ad_plat = obj.getString("ad_plat");
            if ("2".equals(ad_plat)) {
                // 如果 激活事件 同时 不是首次激活时间  就可以不用查询了  因为查了我后面也不会统计
                if ("install_active".equals(last_active_event)&&first_time_install_active_time!=last_active_time) {
                    row = obj.toJSONString();
                }else{
                    JSONObject ad_properties = obj.getJSONObject("ad_properties");
                    String gdt_vid = ad_properties.getString("gdt_vid");

                    //可以 取最近 24小时的 数据
                    String s = last_active_time.split(" ")[0];

                    if (!StringUtils.isEmpty(gdt_vid)) {
                        sql = "SELECT * FROM  ad.ad_callback_mini WHERE impression_id=? ";
                        preparedStatement = conn.prepareStatement(sql);
                        preparedStatement.setString(1, gdt_vid);
                        resultSet = preparedStatement.executeQuery();
                        if (resultSet.next()) {//只取一个
                            String callback = resultSet.getString("callback");
                            String adgroup_id = resultSet.getString("adgroup_id");
                            String ad_id = resultSet.getString("ad_id");
                            ad_properties.put("callback", callback);
                            ad_properties.put("_adgroup_id", adgroup_id);
                            ad_properties.put("_ad_id", ad_id);
                            obj.put("ad_properties", ad_properties.toJSONString());
                            row = obj.toJSONString();
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("执行sql异常: " + e.toString());
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("关闭conn异常: " + e.toString());
            }
        }
        return row;
    }

    public static void main(String[] args) {
        ACTAdFormatUserJoinClick obj = new ACTAdFormatUserJoinClick();
        String row = "{\"monitor_id\":129,\"ad_type\":1,\"account_infos\":\"{\\\"sdk_login_ijJ3nnXvk2Sm22Ml-wSsPw\\\":\\\"{\\\\\\\"ad_type\\\\\\\":1,\\\\\\\"register_time\\\\\\\":\\\\\\\"2022-08-31 08:30:39\\\\\\\"}\\\"}\",\"package_id\":\"24\",\"batchid\":\"20220831083300\",\"last_event_properties\":\"{\\\"account_id\\\":0,\\\"ad_channel\\\":\\\"wechat\\\",\\\"app_version\\\":\\\"v1.7.0826.08261814.49\\\",\\\"area_id\\\":0,\\\"carrier\\\":\\\"iPhone\\\",\\\"device_id\\\":\\\"1962148411-1661872294070\\\",\\\"device_model\\\":\\\"iPhone 11\\\\u003ciPhone12,1\\\\u003e\\\",\\\"device_type\\\":\\\"weixin\\\",\\\"distinct_id\\\":\\\"1661872294057116191184\\\",\\\"eventname\\\":\\\"register\\\",\\\"game_id\\\":202,\\\"group_id\\\":0,\\\"ip\\\":\\\"act-live-public.123u.com\\\",\\\"net_work_type\\\":\\\"wifi\\\",\\\"os\\\":\\\"iOS\\\",\\\"os_version\\\":\\\"15.2\\\",\\\"platform\\\":\\\"sdk_login\\\",\\\"real_ip\\\":\\\"124.222.67.221\\\",\\\"record_time\\\":\\\"2022-08-31 08:30:39\\\",\\\"role_id\\\":0,\\\"role_level\\\":0,\\\"role_name\\\":\\\"\\\",\\\"sdk_account_id\\\":\\\"sdk_login_ijJ3nnXvk2Sm22Ml-wSsPw\\\"}\",\"last_active_event\":\"install_active\",\"last_active_time\":\"2022-08-31 09:30:39\",\"account_id\":\"sdk_login_ijJ3nnXvk2Sm22Ml-wSsPw\",\"distinct_id\":\"1661872294057116191184\",\"ad_properties\":\"{\\\"weixinadinfo\\\":\\\"6400024583.bjgm63ot5akgq02.0.0\\\",\\\"ad_account_id\\\":\\\"25305860\\\",\\\"channel\\\":\\\"129\\\",\\\"gdt_vid\\\":\\\"bjgm63ot5akgq02\\\",\\\"package_id\\\":\\\"24\\\",\\\"media_system_id\\\":\\\"2\\\"}\",\"ad_plat\":\"2\",\"state_properties\":\"{\\\"first_time_active_time\\\":\\\"2022-08-31 08:30:39\\\",\\\"device_os\\\":\\\"iOS\\\",\\\"match_state\\\":1}\",\"game_code\":\"ACT\"}";
        System.out.println(obj.eval(row));
    }


}