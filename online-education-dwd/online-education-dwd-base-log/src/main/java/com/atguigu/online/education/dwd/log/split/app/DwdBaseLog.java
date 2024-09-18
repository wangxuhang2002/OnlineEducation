package com.atguigu.online.education.dwd.log.split.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.online.education.common.base.BaseAPP;
import com.atguigu.online.education.common.constant.Constant;
import com.atguigu.online.education.common.util.DateFormatUtil;
import com.atguigu.online.education.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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
        // 1. 分流 剔除 脏数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        // 将 脏数据流 单独分开
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        // 将 脏数据流 写入 kafka
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);

        // 2. 修复 新老访客标签
        KeyedStream<JSONObject, String> midKeyedDS
                = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> midFixedDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        String lastVisitDate = lastVisitDateState.value();
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) { // is_new 标签为 1
                            if (StringUtils.isEmpty(lastVisitDate)) { // 状态为空，直接将 数据时间 写入 状态
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) { // 状态时间 与 数据时间 不相等，is_new 标记为 0
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else { // is_new 标签为 0
                            if (StringUtils.isEmpty(lastVisitDate)) { // 状态为空，直接将 数据时间 -1天 写入 状态
                                long yesterdayTs = ts - 24 * 60 * 60 * 1000;
                                String yesterday = DateFormatUtil.tsToDate(yesterdayTs);
                                lastVisitDateState.update(yesterday);
                            }
                        }

                        out.collect(jsonObj);
                    }
                }
        );

        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> appVideoTag = new OutputTag<String>("appVideoTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};

        SingleOutputStreamOperator<String> pageDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, String>.Context ctx, Collector<String> out) throws Exception {

                        // ----->错误日志<-----
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        JSONObject appVideoJsonObj = jsonObj.getJSONObject("appVideo");

                        if (startJsonObj != null) {
                            // ----->启动日志<-----
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else if (appVideoJsonObj != null) {
                            // ----->视频日志<-----
                            ctx.output(appVideoTag, jsonObj.toJSONString());
                        } else {
                            // ----->页面日志<-----
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            // ----->曝光日志<-----
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {

                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);

                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    newDisplayJsonObj.put("display", displayJsonObj);

                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            // ----->事件日志<-----
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {

                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);

                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("ts", ts);
                                    newActionJsonObj.put("display", actionJsonObj);

                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            // ----->页面日志<-----
                            out.collect(jsonObj.toJSONString());
                        }

                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> appVideoDS = pageDS.getSideOutput(appVideoTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print("page:");
        errDS.print("err:");
        startDS.print("start:");
        appVideoDS.print("appVideo:");
        displayDS.print("display:");
        actionDS.print("action:");

        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        appVideoDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_APP_VIDEO));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }
}
