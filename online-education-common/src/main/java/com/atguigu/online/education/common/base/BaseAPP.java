package com.atguigu.online.education.common.base;

import com.atguigu.online.education.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseAPP {

    public void start(int port, int parallelism, String ckAndGroupId, String topic) {

        // 1. 基本环境准备
        // 1.1 流处理环境 本地WebUI端口号
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2 并行度设置
        env.setParallelism(parallelism);
        // 1.3 设置 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 2. 检查点设置
        // 2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2.2 设置 检查点 超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
        // 2.3 设置 两检查点 之间 最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);

        // 3. 从kafka读取数据
        // 3.1 获取kafkaSource
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);
        // 3.2 将数据读取，封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 4. 业务逻辑
        handle(env, kafkaStrDS);

        // 5. 提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}