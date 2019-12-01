package com.aia.sink;

import com.aia.handle.HandleTables;
import com.aia.utils.DataSourceUtil;
import com.aia.utils.ParseJsonData;
import com.aia.utils.SqlProxy;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

public class PolarDBSinkFunction2 extends RichSinkFunction<String> {

    private Connection conn = null;
    private SqlProxy sqlProxy = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        conn = DataSourceUtil.getConnection();
        sqlProxy = new SqlProxy();

    }

    @Override
    public void close() throws Exception {
        super.close();
        sqlProxy.shutdown(conn);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        JSONObject jsonObject = ParseJsonData.getJsonData(value);
        String tableName = jsonObject.getString("tableName");

        //判断表名进入相应的方法
        switch (tableName){

            case "t_customer" :
                HandleTables.handleTable_ETA_T_Customer(conn, sqlProxy, jsonObject, tableName);
                break;
            default:
                HandleTables.handleNoTableNameException(conn, sqlProxy, jsonObject, tableName);
        }


    }
}
