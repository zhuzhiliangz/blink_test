package com.aia.utils;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//抽象类，用于回调查询结果


/**
 * 执行sql语句的代理类
 * 提供了插入、删除、更新、查询、关闭连接的方法
 * executeUpdate 插入、删除、更新
 * executeQuery 查询
 * getBeanList 直接将查询结果的一行封装成对象，加入到List中返回
 * shutDown 关闭连接
 */
public class SqlProxy {


    private ResultSet rs = null;
    private PreparedStatement pst = null;

    /**
     * TODO 该方法可执行插入、删除、更新
     * @param conn jdbc连接
     * @param sql  sql语句
     * @param args 动态形参，sql语句中对应的占位符的值
     * @return 执行sql语句后影响的行数，int
     */
    public int executeUpdate(Connection conn, String sql, Object... args) {

        try {
            //1.预编译sql
            pst = conn.prepareStatement(sql);

            //2、设置？的值
            if (args != null && args.length > 0) {
                for (int i = 0; i < args.length; i++) {//数组的下标从0开始
                    pst.setObject(i + 1, args[i]);//？的序号从1开始
                }
            }

            //3、执行更新sql
            return pst.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param conn
     * @param sql
     * @param list 动态形参改为一个List集合
     * @return
     */
    public int executeUpdate2(Connection conn, String sql, List<Object> list) {

        try {
            //1.预编译sql
            pst = conn.prepareStatement(sql);

            //2、设置？的值
            if(list != null && list.size() > 0){
                for(int i = 0; i < list.size(); i++){//List的下标从0开始
                    pst.setObject(i + 1, list.get(i));//？的序号从1开始
                }
            }

            //3、执行更新sql
            return pst.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * TODO 该方法执行查询操作
     * @param conn
     * @param sql
     * @param queryCallBack 抽象方法，用于回调查询结果
     * @param args
     */
        public void executeQuery(Connection conn, String sql, QueryCallBack queryCallBack, Object...args){

            try {
                //1.预编译
                pst = conn.prepareStatement(sql);

                //2、设置？的值
                if (args != null && args.length > 0) {
                    for (int i = 0; i < args.length; i++) {//数组的下标从0开始
                        pst.setObject(i + 1, args[i]);//？的序号从1开始
                    }
                }

                //3、执行查询
                ResultSet rs = pst.executeQuery();
                queryCallBack.process(rs);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        public ResultSet executeQuery2(Connection conn, String sql,  Object...args){

            try {
                //1.预编译
                pst = conn.prepareStatement(sql);

                //2、设置？的值
                if (args != null && args.length > 0) {
                    for (int i = 0; i < args.length; i++) {//数组的下标从0开始
                        pst.setObject(i + 1, args[i]);//？的序号从1开始
                    }
                }

                //3、执行查询
               return pst.executeQuery();


            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

    //T可能代表Department，Employee等各种Javabean的对象
    //clazz是决定你查询的结果是Department，Employee类型中的那个对象

    /**
     *
     * @param clazz
     * @param conn
     * @param sql
     * @param args
     * @param <T> 将查询结果一行封装成一个对象的类型
     * @return 由查询结果封装一个个对象的集合
     */
    public <T> ArrayList<T> getBeanList(Class<T> clazz, Connection conn, String sql, Object... args){

        try {
            pst = conn.prepareStatement(sql);

            //3、设置？的值
            if(args!=null && args.length>0){
                for (int i = 0; i < args.length; i++) {//数组的下标从0开始
                    pst.setObject(i+1, args[i]);//？的序号从1开始
                }
            }

            //4、执行查询
            rs = pst.executeQuery();

            //获取结果集的元数据对象，该对象有对结果集的数据进行描述的相关信息
            ResultSetMetaData rsm = rs.getMetaData();
            //(1)获取结果集的列数
            int count = rsm.getColumnCount();

            ArrayList<T> list = new ArrayList<T>();
            //todo 把ResultSet结果集中的数据封装到一个一个JavaBean对象中，并且存到list中
            while(rs.next()){//循环一次，代表一行，一行就是一个JavaBean对象
                //(2)创建一个JavaBean的对象
                T obj = clazz.newInstance();

                //有几列，就代表有几个属性
                //为obj的每一个属性赋值
                for (int i = 0; i < count; i++) {
                    //通过反射设置属性的值
                    //todo (3)从结果集的元数据对象中获取第几列的字段名
                    String columnName = rsm.getColumnLabel(i+1);//mysql的序号从1开始

                    //(4)获取属性对象
                    Field field = clazz.getDeclaredField(columnName);//根据字段名称，获取属性对象

                    //(5)设置属性可以被访问
                    field.setAccessible(true);

                    //todo (6)设置属性的值
                    field.set(obj, rs.getObject(i+1));//从结果集中获取第i+1的值，赋值给该属性
                }

                list.add(obj);
            }
            //7、返回结果
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //TODO 获取单条记录封装到对象中
    public <T> T getBean(Class<T> type, Connection conn, String sql, Object... params) {
        return getBeanList(type,conn,sql,params).get(0);
    }


    //TODO 关闭连接
    public void shutdown(Connection conn){
            DataSourceUtil.closeResource(rs, pst, conn);
        }
}
