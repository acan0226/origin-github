package net.acan.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import net.acan.gmall.realtime.bean.OrderInfo;
import net.acan.gmall.realtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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

    public static <T>List<T> queryList(Connection conn,
                                             String sql,
                                             Object[] args,
                                             Class<T> tclass) throws Exception {
        ArrayList<T> result = new ArrayList<>();

        PreparedStatement ps = conn.prepareStatement(sql);
    //给sql中的占位符赋值
        for (int i = 0; args != null && i < args.length; i++) {
            //遍历每个参数，给占位符赋值
            Object arg = args[i];
            ps.setObject(i+1,arg);
        }
        //开始执行查询,resultSet是结果集
        ResultSet resultSet = ps.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        // next方法用来判断结果集中的数据, 如果有就把指针移动到这一行
        while (resultSet.next()) {
            //进来一次等于遍历到结果的一行
            //把这一行数据中的所有列全部取出, 封装到一个T类型的对象只
            T t = tclass.newInstance();// 中T类型的无参构造器创建一个对象, 把这一行的数据封装到这个对象中, 每一列就是对象中的一个属性
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                // 获取列名就是 T对象的属性名
                // 获取列的值就是对应的 属性的值
                //getColumnLabel获取列的别名，getColumnName获取列的名称
                String columnName = metaData.getColumnLabel(i);
                Object columnValue = resultSet.getObject(i);
                //beanutils 工具类 我们可以很方便的对bean对象的属性进行操作
                BeanUtils.setProperty(t,columnName,columnValue);

            }
            result.add(t);

        }
            ps.close();
        return result;
    }

    public static void main(String[] args) throws Exception {
        //先查mysql
        //Connection conn = getJDBCConnection("com.mysql.jdbc.Driver", "jdbc:mysql://hadoop162:3306/gmall2022", "root", "aaaaaa");
        //连接phoenix
        Connection conn = getJDBCConnection(Constant.PHONEIX_DRIVER, Constant.PHONEIX_URL, null, null);

        //List<JSONObject> list = queryList(conn, "select * from order_info", null, JSONObject.class);
//        List<OrderInfo> list = queryList(conn, "select * from order_info", null, OrderInfo.class);
        List<JSONObject> list = queryList(conn, "select * from dim_sku_info where id =?", new Object[]{"1"}, JSONObject.class);
        for (JSONObject obj : list) {
            System.out.println(obj);
        }
    }

}
