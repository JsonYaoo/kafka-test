package com.jsonyao.cs.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 测试Canal整合Kafka: 要注意顺序消费的问题
 */
public class CollectKafkaConsumer {

    public static void main(String[] args) {
        String topic = "test_db.test_table";
        CollectKafkaConsumer collectKafkaConsumer = new CollectKafkaConsumer(topic);
        collectKafkaConsumer.receive();
    }

    /**
     * Consumer实例
     */
    private KafkaConsumer<String, String> consumer;

    /**
     * Consumer构造方法
     * @param topic
     */
    public CollectKafkaConsumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-group-id");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    private void receive(){
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);

                /**
                 * 获取topic: test-db.demo, 分区位置: 2, 消息数为:1
                 */
                String topic = topicPartition.topic();
                int size = records.size();
                System.err.println("获取topic: " + topic + ", 分区位置: " + topicPartition.partition() + ", 消息数为:" + size);

                /**
                 * *-> 重要Value属性: Canal自己定义的Kafka消息实体
                 * {
                 *      *-> "data":[{"ID":"3","NAME":"ddd","AGE":"31"}],
                 *      *-> "database":"test_db",
                 *      "es":1612979367000,
                 *      *-> "id":2,
                 *      "isDdl":false,
                 *      (需要去掉, 占数据量)*-> "mysqlType":{"ID":"int","NAME":"varchar(20)","AGE":"int"},
                 *      *-> "old":[{"ID":"2","NAME":"ccc","AGE":"22"}],
                 *      *-> "pkNames":null,
                 *      "sql":"",
                 *      "sqlType":{"ID":4,"NAME":12,"AGE":4},
                 *      *-> "table":"test_table",
                 *      *-> "ts":1612979366198,
                 *      *-> "type":"UPDATE"
                 * }
                 */
                for(int i = 0; i < size; i++){
                    System.err.println("-----> value: " + records.get(i).value());
                    long offset = records.get(i).offset() + 1;
                    consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset)));
                    System.err.println("同步成功, topic: " + topic+ ", 提交的 offset: " + offset);
                }
            }
        }
    }
}
