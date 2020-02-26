package com.mchou.untils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSender {

    //定义kafka的producer生产者
    private static KafkaProducer<String, String> kafkaProducer;

    //使用kafka生产者将canal监测到的数据发送数据至kafka
    public static void send(String topic, String data) {

        if (kafkaProducer == null) {

            kafkaProducer = createKafkaProducer();
        }

        //发送数据
        kafkaProducer.send(new ProducerRecord<>(topic,data));
    }

    //创建kafka生产者对象
    private static KafkaProducer<String, String> createKafkaProducer() {

        //创建kafka生产者的配置信息
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop106:9092,hadoop107:9092,hadoop108:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<>(properties);

        return kafkaProducer;
    }
}
