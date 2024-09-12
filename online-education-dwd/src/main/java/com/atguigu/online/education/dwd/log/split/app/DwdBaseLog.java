package com.atguigu.online.education.dwd.log.split.app;


import com.atguigu.online.education.common.base.BaseAPP;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdBaseLog extends BaseAPP {

    public static void main(String[] args) {
        new DwdBaseLog().start(
                10011,
                4,
                "dwd_base_log",
                "topic_log"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        /*OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    }
                }
        )*/
    }
}
