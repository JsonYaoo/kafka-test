package com.jsonyao.cs.api.serial;

import com.alibaba.fastjson.JSON;
import com.jsonyao.cs.api.constant.Const;
import com.jsonyao.cs.api.entity.User;
import com.jsonyao.cs.api.interceptor.CustomProducerInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka Producer拦截器测试类
 */
public class SerializerProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "serial-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
        /*添加生产者拦截器属性: 可以配置多个拦截器*/
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomProducerInterceptor.class.getName());

        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        for(int i = 0; i < 10; i++){
            User user = new User("00" + i, "张三");
            ProducerRecord<String, User> record = new ProducerRecord<>(Const.TOPIC_SERIAL, user);

            producer.send(record);
            System.err.println("serial producer send....");
        }

        producer.close();
    }
}
