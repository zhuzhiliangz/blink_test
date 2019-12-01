package com.aia.handle;

import com.aia.utils.SqlProxy;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class HandleTables {

    public static void handleTable_ETA_T_Customer(Connection conn, SqlProxy sqlProxy, JSONObject jsonObject, String tableName){

        String operationType = jsonObject.getString("operationType");
        String scn = jsonObject.getString("scn");
        String loaderTime = jsonObject.getString("loaderTime");
        Integer columnNum = jsonObject.getInteger("columnNum");
        String opTs = jsonObject.getString("opTs");
        String seqid = jsonObject.getString("seqid");
        String rowid = jsonObject.getString("rowid");
        //jsonObject.getString("")//优先级从配置文件中获得

        String cid = jsonObject.getString("cid");
        String name = jsonObject.getString("name");
        String ename = jsonObject.getString("ename");
        String gender = jsonObject.getString("gender");
        String idType = jsonObject.getString("idType");
        String idnumber = jsonObject.getString("idnumber");
        String birthday = jsonObject.getString("birthday");
        Integer age = jsonObject.getInteger("age");
        String idType2 = jsonObject.getString("idType2");
        String idnumber2 = jsonObject.getString("idnumber2");
        String insertime = jsonObject.getString("insertime");
        String updatetime = jsonObject.getString("updatetime");
        String srcsys = jsonObject.getString("srcsys");

        List insertList = new ArrayList<Object>();
        List updateList = new ArrayList<Object>();

        if(operationType == "I"){
            String insert_ETA_T_Customer_TableSql = "insert ignore into t_customer() values()";
            sqlProxy.executeUpdate2(conn, insert_ETA_T_Customer_TableSql, insertList);

        }else if(operationType == "D"){
            String delete_ETA_T_Customer_TableSql = "delete from t_customer where cid=?";
            sqlProxy.executeUpdate(conn, delete_ETA_T_Customer_TableSql, cid);

        }else{

        }

    }


    public static void handleNoTableNameException(Connection conn, SqlProxy sqlProxy, JSONObject jsonObject, String tableName){

    }
}
