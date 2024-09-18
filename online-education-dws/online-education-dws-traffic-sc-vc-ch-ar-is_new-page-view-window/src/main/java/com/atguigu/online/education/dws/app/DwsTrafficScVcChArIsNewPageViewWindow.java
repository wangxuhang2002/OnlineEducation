package com.atguigu.online.education.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.online.education.common.base.BaseAPP;
import com.atguigu.online.education.common.bean.TrafficPageViewBean;
import com.atguigu.online.education.common.constant.Constant;
import com.atguigu.online.education.common.function.BeanToJsonStrMapFunction;
import com.atguigu.online.education.common.util.DateFormatUtil;
import com.atguigu.online.education.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class DwsTrafficScVcChArIsNewPageViewWindow extends BaseAPP {

    public static void main(String[] args) {
        new DwsTrafficScVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_sc_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 1. 将 json字符串 转为 json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

//        jsonObjDS.print();
        // {"common":{"sc":"1","ar":"20","uid":"2554","os":"Android 11.0","ch":"wandoujia","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_190","vc":"v2.1.132","ba":"Xiaomi","sid":"a051a278-0e0c-41bd-b175-53aa3001a675"},"page":{"page_id":"course_detail","item":"43","during_time":8357,"item_type":"course_id","last_page_id":"course_list"},"ts":1726196659028}
        // 2. 按 mid 分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // 3. 对分组后数据处理，转换 类型，统计 独立访客数
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        stringValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(stringValueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        // ----->获取 流中数据<-----
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        String sc = commonJsonObj.getString("sc");
                        String vc = commonJsonObj.getString("vc");
                        String ch = commonJsonObj.getString("ch");
                        String ar = commonJsonObj.getString("ar");
                        String isNew = commonJsonObj.getString("is_new");
                        String sid = commonJsonObj.getString("sid");
                        // ----->获取 度量值<-----
                        // 独立访客数
                        long uvCt = 0L;
                        String lastVisitDate = lastVisitDateState.value();

                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }
                        // 持续时间
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        Long duringTime = pageJsonObj.getLong("during_time");

                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                sc,
                                vc,
                                ch,
                                ar,
                                isNew,
                                uvCt,
                                null,
                                1L,
                                duringTime,
                                ts,
                                sid
                        );

                        out.collect(trafficPageViewBean);
                    }
                }
        );
        // 4. 按 sid 分组
        KeyedStream<TrafficPageViewBean, String> sidKeyedDS = beanDS.keyBy(TrafficPageViewBean::getSid);
        // 5. 对分组后数据处理，统计 会话数
        SingleOutputStreamOperator<TrafficPageViewBean> sidBeanDS = sidKeyedDS.process(
                new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {

                    private ValueState<Boolean> sidTagState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> stringValueStateDescriptor = new ValueStateDescriptor<Boolean>("sidTagState", Boolean.class);
                        stringValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
                        sidTagState = getRuntimeContext().getState(stringValueStateDescriptor);
                    }

                    @Override
                    public void processElement(TrafficPageViewBean beanObj, KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        Boolean sidTag = sidTagState.value();
                        if (sidTag == null) {
                            beanObj.setSvCt(1L);
                            sidTagState.update(true);

                        } else {
                            beanObj.setSvCt(0L);
                        }

                        out.collect(beanObj);
                    }
                }
        );
        // 7. 设置 水位线，指定 事件时间
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = sidBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
        // 8. 按 统计维度 分组
        KeyedStream<TrafficPageViewBean, Tuple5<String, String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple5.of(
                                bean.getSc(),
                                bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew()
                        );
                    }
                }
        );
        // 9. 开窗
        WindowedStream<TrafficPageViewBean, Tuple5<String, String, String, String, String>, TimeWindow> windowDS = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        // 10. 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());

                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple5<String, String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple5<String, String, String, String, String> stringStringStringStringStringTuple5, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean trafficPageViewBean = input.iterator().next();

                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());

                        trafficPageViewBean.setStt(stt);
                        trafficPageViewBean.setEdt(edt);
                        trafficPageViewBean.setCur_date(curDate);

                        out.collect(trafficPageViewBean);
                    }
                }
        );

//        reduceDS.print();

        SingleOutputStreamOperator<String> strDS = reduceDS.map(new BeanToJsonStrMapFunction<>());

        strDS.sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_sc_vc_ch_ar_is_new_page_view_window"));
    }
}
