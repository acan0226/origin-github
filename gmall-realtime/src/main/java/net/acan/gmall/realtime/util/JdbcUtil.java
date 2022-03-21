package net.acan.gmall.realtime.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcUtil {
    public static Connection getJDBCConnection(String driver,
                                               String url,
                                               String user,
                                               String password) throws ClassNotFoundException, SQLException {
        //获取jdbc连接
        //1.加载驱动
        Class.forName(driver);
        //2.获取连接
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }

}
