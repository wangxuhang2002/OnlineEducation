package com.atguigu.online.education.common.util;

import com.atguigu.online.education.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {

    // 获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic, String groupId){

        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();

        return kafkaSource;
    }

    public static MySqlSource<String> getMySqlSource(String database,String tableName){
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                //在程序启动的时候，先对指定的库的表进行全表扫描(同步历史数据，和binlog没有关系)，然后在从binlog的最新位置读取变化数据
                //.startupOptions(StartupOptions.initial())
                .databaseList(database)
                .tableList(database + "." + tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
