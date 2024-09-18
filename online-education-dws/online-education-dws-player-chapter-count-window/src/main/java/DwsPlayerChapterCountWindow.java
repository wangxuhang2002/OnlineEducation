import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.online.education.common.base.BaseAPP;
import com.atguigu.online.education.common.bean.DwsLearnchapterPlaywindowBean;
import com.atguigu.online.education.common.function.BeanToJsonStrMapFunction;
import com.atguigu.online.education.common.function.DimAsyncFunction;
import com.atguigu.online.education.common.util.DateFormatUtil;
import com.atguigu.online.education.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.atguigu.online.education.common.constant.Constant.TOPIC_DWD_TRAFFIC_APP_VIDEO;

public class DwsPlayerChapterCountWindow extends BaseAPP {
    public static void main(String[] args) {
        new DwsPlayerChapterCountWindow().start(
                10023,
                4,
                "dws_player_chapter_count_window",
                TOPIC_DWD_TRAFFIC_APP_VIDEO
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //3.转换数据结构
        SingleOutputStreamOperator<DwsLearnchapterPlaywindowBean> playwindowBeanSingleOutputStreamOperator = kafkaStrDS.map(
                new MapFunction<String, DwsLearnchapterPlaywindowBean>() {
                    @Override
                    public DwsLearnchapterPlaywindowBean map(String s) throws Exception {
                        JSONObject JsonObj = JSON.parseObject(s);

                        return DwsLearnchapterPlaywindowBean.builder()
                                .videoId(JsonObj.toJSONString("videoId"))
                                .userId(JsonObj.toJSONString("userId"))
                                .playDuration(JsonObj.getLong("playDuration"))
                                .playCount(JsonObj.getLong("playCount"))
                                .ts(JsonObj.getLong("ts"))
                                .build();
                    }
                }
        );
        //4.按照用户id分组
        KeyedStream<DwsLearnchapterPlaywindowBean, String> keyedDS = playwindowBeanSingleOutputStreamOperator.keyBy(new KeySelector<DwsLearnchapterPlaywindowBean, String>() {
            @Override
            public String getKey(DwsLearnchapterPlaywindowBean dwsLearnchapterPlaywindowBean) throws Exception {
                return dwsLearnchapterPlaywindowBean.getUserId();
            }
        });
        //5.过滤出独立用户数
        SingleOutputStreamOperator<DwsLearnchapterPlaywindowBean> lastLoginDateStateDS =
                keyedDS.process(new KeyedProcessFunction<String, DwsLearnchapterPlaywindowBean, DwsLearnchapterPlaywindowBean>() {
            ValueState<String> lastLoginDateState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<String>("lastLoginDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(DwsLearnchapterPlaywindowBean dwsLearnchapterPlaywindowBean, KeyedProcessFunction<String, DwsLearnchapterPlaywindowBean, DwsLearnchapterPlaywindowBean>.Context context, Collector<DwsLearnchapterPlaywindowBean> collector) throws Exception {
                String lastDt = lastLoginDateState.value();
                String curDt = DateFormatUtil.tsToDate(dwsLearnchapterPlaywindowBean.getTs());
                if (lastDt == null || lastDt.compareTo(curDt) < 0) {
                    dwsLearnchapterPlaywindowBean.setPlayCount(1L);
                    lastLoginDateState.update(curDt);
                } else {
                    dwsLearnchapterPlaywindowBean.setPlayCount(0L);
                }
                collector.collect(dwsLearnchapterPlaywindowBean);
            }
        });
        //6.添加水位线
        SingleOutputStreamOperator<DwsLearnchapterPlaywindowBean> withWaterMarkDS = lastLoginDateStateDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsLearnchapterPlaywindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsLearnchapterPlaywindowBean>() {
                    @Override
                    public long extractTimestamp(DwsLearnchapterPlaywindowBean dwsLearnchapterPlaywindowBean, long l) {
                        return dwsLearnchapterPlaywindowBean.getTs();
                    }
                }));
        //7.开窗
        SingleOutputStreamOperator<DwsLearnchapterPlaywindowBean> reduceDS = withWaterMarkDS.keyBy(DwsLearnchapterPlaywindowBean::getVideoId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<DwsLearnchapterPlaywindowBean>() {
                            @Override
                            public DwsLearnchapterPlaywindowBean reduce(DwsLearnchapterPlaywindowBean value1, DwsLearnchapterPlaywindowBean value2) throws Exception {
                                value1.setPlayCount(value1.getPlayCount() + value2.getPlayCount());
                                value1.setPlayUserCount(value1.getPlayUserCount() + value2.getPlayUserCount());
                                value1.setPlayDuration(value1.getPlayDuration() + value2.getPlayDuration());
                                return value1;
                            }
                        }, new ProcessWindowFunction<DwsLearnchapterPlaywindowBean, DwsLearnchapterPlaywindowBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<DwsLearnchapterPlaywindowBean, DwsLearnchapterPlaywindowBean, String, TimeWindow>.Context context, Iterable<DwsLearnchapterPlaywindowBean> iterable, Collector<DwsLearnchapterPlaywindowBean> collector) throws Exception {
                                String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                for (DwsLearnchapterPlaywindowBean element : iterable) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    collector.collect(element);
                                }
                            }
                        }
                );


        //9.补全维度字段
        SingleOutputStreamOperator<DwsLearnchapterPlaywindowBean> withdimDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsLearnchapterPlaywindowBean>() {
                    @Override
                    public void addDims(DwsLearnchapterPlaywindowBean obj, JSONObject dimJsonObj) {
                        obj.setChapterName(dimJsonObj.getString("chapter_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_chapter_info";
                    }

                    @Override
                    public String getRowKey(DwsLearnchapterPlaywindowBean obj) {
                        return obj.getChapterId();
                    }
                }, 60, TimeUnit.SECONDS
        );


        //10.写到doris中
        withdimDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_player_chapter_count_window"));
    }
}
