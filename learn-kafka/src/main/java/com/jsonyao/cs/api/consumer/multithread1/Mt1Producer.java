package com.jsonyao.cs.api.consumer.multithread1;

import com.alibaba.fastjson.JSON;
import com.jsonyao.cs.api.constant.Const;
import com.jsonyao.cs.api.entity.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka Producer: 测试一Consumer一Partition多线程模型
 */
public class Mt1Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "mt1-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for(int i = 0; i < 10; i++){
            User user = new User("00" + i, "张三");
            ProducerRecord<String, String> record = new ProducerRecord<>(Const.TOPIC_MT1, JSON.toJSONString(user));

            producer.send(record);
            System.err.println("mt1 producer send....");
        }

        producer.close();
    }
}
