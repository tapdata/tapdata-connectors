package io.tapdata.connector.gauss;


import java.sql.*;
 
public class MysqlJdbc {
    final String host;
    final String database;
    final String user;
    final String pwd;
    final int port;
    final String url;

    public MysqlJdbc(String host, int port, String database, String user, String pwd) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pwd = pwd;
        this.database = database;
        this.url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false", host, port, database);
    }

    public Connection getConn(){
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        //数据库URL
        try {
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return conn;
    }
}