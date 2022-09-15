package com.atguigu.utils;

import net.minidev.json.JSONObject;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import javax.annotation.Nullable;
import java.util.Properties;
public class MyKafkaUtil {

    private static final String brokerList = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static String default_topic = "dwd_default_topic";

    //TODO 获取kafka生产者
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<>(
                brokerList,
                topic,
                new SimpleStringSchema()
        );
    }

    //TODO 获取kafka 多主题的生产者
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return new FlinkKafkaProducer<T>(
                default_topic,
                kafkaSerializationSchema,
                        props,
                        FlinkKafkaProducer.Semantic.NONE);

                }


    //TODO 获取kafka消费者

    public static FlinkKafkaConsumer<String> getKafkaconsumer(String topic, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                props
        );
    }



}
