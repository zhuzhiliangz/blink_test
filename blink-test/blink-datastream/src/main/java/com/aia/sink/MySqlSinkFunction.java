package com.aia.sink;

import com.aia.utils.DataSourceUtil;
import com.aia.utils.ParseJsonData;
import com.aia.utils.QueryCallBack;
import com.aia.utils.SqlProxy;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlSinkFunction extends RichSinkFunction<String> {

    private Connection conn = null;
    private String insertSql = "";
    private String updataSql = "";
    private String selectSql = "";
    private String isExistCid = null;
    private SqlProxy sqlProxy = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        conn = DataSourceUtil.getConnection();
        sqlProxy = new SqlProxy();
        insertSql = "INSERT INTO flink_policy (polnum, `type`, seqno, cid, relationship, share, tableName, scn) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        updataSql = "UPDATE flink_policy SET polnum=?, `type`=?, seqno=?, relationship=?, share=?, tableName=?, scn=?  WHERE cid=?";
        selectSql = "select polnum from flink_policy where cid=?";

    }

    @Override
    public void close() throws Exception {
        super.close();
        sqlProxy.shutdown(conn);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        System.out.println("1--" + System.currentTimeMillis());
        JSONObject jsonObject = ParseJsonData.getJsonData(value);

        if(jsonObject != null){
            String polnum = jsonObject.getString("polnum");
            String type = jsonObject.getString("type");
            int seqno = jsonObject.getInteger("seqno");
            String cid = jsonObject.getString("cid");
            String relationship = jsonObject.getString("relationship");
            double share = jsonObject.getDouble("share");
            String tableName = jsonObject.getString("tableName");
            String scn = jsonObject.getString("scn");


            selectSql += cid;
            System.out.println("2--" + System.currentTimeMillis());
           // String isExistCid = null;
            sqlProxy.executeQuery(conn, selectSql, new QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws SQLException {
                    while (rs.next()){
                        isExistCid = rs.getString(1);

                    }
                }
            });

            if(isExistCid != null){
                int count = sqlProxy.executeUpdate(conn, updataSql, polnum, type, seqno, relationship, share, tableName, "3566", cid);
            }
            System.out.println("3--" + System.currentTimeMillis());
//
//            if(count == 0){
//                sqlProxy.executeUpdate(conn, insertSql,polnum, type, seqno, cid, relationship, share, tableName, scn);
//                sqlProxy.executeUpdate(conn, insertSql,polnum, type, seqno, cid, relationship, share, tableName, scn);
//            }

        }
    }
}
