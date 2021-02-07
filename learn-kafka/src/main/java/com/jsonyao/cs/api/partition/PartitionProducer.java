package com.jsonyao.cs.api.partition;

import com.jsonyao.cs.api.constant.Const;
import com.jsonyao.cs.api.entity.User;
import com.jsonyao.cs.api.interceptor.CustomProducerInterceptor;
import com.jsonyao.cs.api.serial.UserSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka Producer测试类: 自定义Producer Partition
 */
public class PartitionProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "partition-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomProducerInterceptor.class.getName());
        /*添加自定义分区器*/
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        for(int i = 0; i < 10; i++){
            User user = new User("00" + i, "张三");
            ProducerRecord<String, User> record = new ProducerRecord<>(Const.TOPIC_PARTITION, user);

            producer.send(record);
            System.err.println("partition producer send....");
        }

        producer.close();
    }
}
