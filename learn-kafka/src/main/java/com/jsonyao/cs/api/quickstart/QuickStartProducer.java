package com.jsonyao.cs.api.quickstart;

import com.alibaba.fastjson.JSON;
import com.jsonyao.cs.api.constant.Const;
import com.jsonyao.cs.api.entity.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka Producer急速入门测试类
 */
public class QuickStartProducer {

    public static void main(String[] args) {
        // 1. 配置生产者启动的关键属性参数
        Properties properties = new Properties();
        /*1.1. 连接kafka集群的服务列表，如果有多个，使用逗号进行分隔*/
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        /*1.2. 标记kafkaClient的ID*/
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "quickstart-producer");
        /*1.3. Key序列化器: kafka用于做消息投递计算具体投递到对应的主题的哪一个partition而需要的*/
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /*1.4. Value序列化器: 实际发送消息的内容序列化*/
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. 传递properties属性参数集合, 构造kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for(int i = 0; i < 10; i++){
            // 3. 构造消息内容: topic, 实际消息体
            User user = new User("00" + i, "张三");
            ProducerRecord<String, String> record = new ProducerRecord<>(Const.TOPIC_QUICKSTART, JSON.toJSONString(user));

            // 4. 发送消息
            producer.send(record);
            System.err.println("quickstart producer send....");
        }

        // 5. 关闭生产者
        producer.close();
    }
}
