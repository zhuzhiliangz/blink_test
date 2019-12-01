package com.aia.app;

import com.aia.sink.MySqlSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class PolicyTest {

    public static void main(String[] args) throws Exception {

        // 创建Execution Environment。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过从本地TXT文件读取数据。
        //DataStream<String> text = env.readTextFile("./test.txt");
        DataStream<String> text = env.fromElements("{\"polnum\":\"35426\",\"type\":\"T\",\"seqno\":\"8970\",\"cid\":\"35786\",\"relationship\":\"J\",\"share\":\"0.88\",\"tableName\":\"flink_policy\",\"scn\":\"35426\"}");

        text.addSink(new MySqlSinkFunction());

        env.execute("policy test");
    }
}
