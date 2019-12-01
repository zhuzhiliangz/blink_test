package com.aia.sink;

import com.aia.utils.DataSourceUtil;
import com.aia.utils.ParseJsonData;
import com.aia.utils.SqlProxy;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

public class PolarDBSinkFunction extends RichSinkFunction<Tuple2<String,Long>> {

    private Connection conn = null;
    private String insertSql = "";
    private String updataSql = "";
    private SqlProxy sqlProxy = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DataSourceUtil.getConnection();
        sqlProxy = new SqlProxy();
        insertSql = "INSERT INTO flink_policy (polnum, `type`, seqno, cid, relationship, share, tableName, scn) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        updataSql = "UPDATE flink_policy SET polnum=?, `type`=?, seqno=?, relationship=?, share=?, tableName=?, scn=?  WHERE cid=?";

    }

    @Override
    public void close() throws Exception {
        super.close();
        sqlProxy.shutdown(conn);
    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {

        String jsonString = value.f0;
        JSONObject jsonObject = ParseJsonData.getJsonData(jsonString);

        if(jsonObject != null){
            String polnum = jsonObject.getString("polnum");
            String type = jsonObject.getString("type");
            int seqno = jsonObject.getInteger("seqno");
            String cid = jsonObject.getString("cid");
            String relationship = jsonObject.getString("relationship");
            double share = jsonObject.getDouble("share");
            String tableName = jsonObject.getString("tableName");
            String scn = jsonObject.getString("scn");

            int count = sqlProxy.executeUpdate(conn, updataSql, polnum, type, seqno, relationship, share, tableName, scn, cid);

            if(count == 0){
                sqlProxy.executeUpdate(conn, insertSql,polnum, type, seqno, cid, relationship, share, tableName, scn);
            }

        }
    }
}
