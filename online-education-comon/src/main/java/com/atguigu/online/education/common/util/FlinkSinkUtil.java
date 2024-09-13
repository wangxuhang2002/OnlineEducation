package com.atguigu.online.education.common.util;

import com.atguigu.online.education.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Properties;

public class FlinkSinkUtil {

    // 获取 KafkaSink
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("xxx")
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 +"")
                .build();

        return kafkaSink;
    }

    // 获取 DorisSink
    public static DorisSink<String> getDorisSink(String tableName) {

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");

        DorisSink<String> dorisSink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.DORIS_FE_NODES + "." + tableName)
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
//                        .setLabelPrefix("doris-label")
                        .disable2PC()
                        .setDeletable(false)
                        .setMaxRetries(3)
                        .setStreamLoadProp(props)
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();

        return dorisSink;
    }
}
