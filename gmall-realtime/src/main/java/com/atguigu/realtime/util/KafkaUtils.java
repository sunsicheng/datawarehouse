package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/30 21:13
 */
public class KafkaUtils {

    public static FlinkKafkaConsumer<String> getKafkaConsume(String topic, String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }


    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("transaction.timeout.ms", 1000 * 60 * 15 + "");
        return new FlinkKafkaProducer<String>(
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, null, element.getBytes());
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

    }


    /***
     * 动态根据数据，写到不同的topic
     * @return
     */
    public static FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>> kafkaSinkAuto() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        //如果15分钟没有更新状态，则超时 默认1分钟
        props.setProperty("transaction.timeout.ms", 1000 * 60 * 15 + "");

        return new FlinkKafkaProducer<Tuple2<JSONObject,TableProcess>>(
                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject,TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element, @Nullable Long timestamp) {
                        String topic = element.f1.getSinkTable();
                        return new ProducerRecord<>(topic,element.f0.toJSONString().getBytes());
                    }
                }
                ,
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

    }

}
