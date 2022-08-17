package com.hlhy.udfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hlhy.utils.ConnectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.*;
import java.util.List;

public class GetInstallActiveTime extends ScalarFunction {
    public GetInstallActiveTime() {
    }
    public Connection starRocksConn;
    //只处理 激活事件  同时确保是首次激活
    public String eval(String game_code,String distinct_id,String last_active_time,String jsonstr) {
        return eval(game_code, distinct_id, last_active_time, jsonstr,"device_install_active_info");
    }

    public String eval(String game_code,String distinct_id,String last_active_time,String jsonstr,String tablename) {
        String res = "1970-01-01 00:00:00";
        String message=game_code+"\t"+distinct_id+"\t"+last_active_time;
        if(StringUtils.isEmpty(game_code)||StringUtils.isEmpty(distinct_id)||StringUtils.isEmpty(distinct_id)){
            return  res;
        }
        try {
            starRocksConn = DriverManager.getConnection("jdbc:mysql://172.17.0.22:3306/ad?characterEncoding=utf-8", "root", "Bigdata.2021!@#");
            //starRocksConn = DriverManager.getConnection("jdbc:mysql://10.16.0.8:9030/ad?characterEncoding=utf-8", "root", "root123");

            String sql = "SELECT * FROM "+tablename+" WHERE game_code=? and distinct_id=?";
            PreparedStatement preparedStatement = starRocksConn.prepareStatement(sql);
            preparedStatement.setString(1, game_code);
            preparedStatement.setString(2, distinct_id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {//只取一个
                res=resultSet.getString("last_active_time");
                System.out.println("已经有数据\t"+message+"\t"+res);
            } else {
                String updatesql = "insert into "+tablename+"(game_code,distinct_id,last_active_time,jsonstr) values(?,?,?,?)";
                //数据库没有数据  插入一条记录
                preparedStatement = starRocksConn.prepareStatement(updatesql);
                preparedStatement.setString(1, game_code);
                preparedStatement.setString(2, distinct_id);
                preparedStatement.setString(3, last_active_time);
                preparedStatement.setString(4, jsonstr);
                int isOk = preparedStatement.executeUpdate();
                if (isOk > 0) {
                    System.out.println("插入成功\t"+message);
                }else {
                    System.out.println("插入失败\t"+message);
                }
                res = last_active_time;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("执行sql异常: " + e.toString());
        } finally {
            try {
                starRocksConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("关闭conn异常: " + e.toString());
            }
        }
        return  res;
    }


    public static void main(String[] args) {
        GetInstallActiveTime isExists =new GetInstallActiveTime();
        String game_code="ACT";
        String distinct_id="aaa165906499184414309710111";
        String last_active_time="2022-07-29 12:48:55";
        String jsonstr="";
        System.out.println(isExists.eval(game_code,distinct_id,last_active_time,jsonstr,"device_install_active_info"));
    }


}