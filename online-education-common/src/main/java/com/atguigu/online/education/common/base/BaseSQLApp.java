package com.atguigu.online.education.common.base;

import com.atguigu.online.education.common.constant.Constant;
import com.atguigu.online.education.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {

    public void start(int port, int parallelism, String ck){
        // 1. 基本环境准备
        // 1.1 流处理环境 本地WebUI端口号
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2 并行度设置
        env.setParallelism(parallelism);
        // 1.3 设置 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 1.4 表 执行环境 准备
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 检查点设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2.2 设置 检查点 超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
        // 2.3 设置 两检查点 之间 最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);

        handle(env, tableEnv);
    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);

    public void readOdsDb(StreamTableEnvironment tableEnv,String groupId) {
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` string,\n" +
                "  `table` string,\n" +
                "  `type` string,\n" +
                "  `data` map<string,string>,\n" +
                "  `old` map<string,string>,\n" +
                "  ts bigint,\n" +
                "  pt as proctime(),\n" +
                "  et as TO_TIMESTAMP_LTZ(ts, 0),\n" +
                "  WATERMARK FOR et AS et \n" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DB,groupId));
    }

}