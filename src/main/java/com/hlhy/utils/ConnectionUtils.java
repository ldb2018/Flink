package com.hlhy.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import redis.clients.jedis.Jedis;

import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionUtils {

    public static java.sql.Connection getMysqlConnection(ParameterTool parameterTool) throws SQLException {
        java.sql.Connection mysqlConn = DriverManager.getConnection(parameterTool.getRequired("mysql.url"), parameterTool.getRequired("mysql.user"), parameterTool.getRequired("mysql.passwd"));
        return mysqlConn;
    }

    public static java.sql.Connection getStarRocksConn(ParameterTool parameterTool) throws Exception{

        String url = parameterTool.getRequired("dorisdb.url");
        String username = parameterTool.getRequired("dorisdb.user");
        String password = parameterTool.getRequired("dorisdb.pwd");
        return DriverManager.getConnection(url, username, password);

    }

    public static Jedis getJedis(ParameterTool parameterTool) {
        String host = parameterTool.getRequired("redis.host");
        int port = parameterTool.getInt("redis.port", 6379);
        String password = parameterTool.getRequired("redis.password");
        int db = parameterTool.getInt("redis.db", 0);
        Jedis jedis = new Jedis(host, port);
        jedis.auth(password);
        jedis.select(db);
        return jedis;
    }
}
