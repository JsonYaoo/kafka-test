package com.jsonyao.cs.api.consumer.core;

import com.jsonyao.cs.api.constant.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Consumer: 测试消费者拉取位置
 */
public class PullConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "core-group1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        /**
         * 设置消费者每次拉取位置: 即从什么位置开始拉取消息
         *      1) none: 当前消费组, 各分区offset都已提交时, 从提交的offset开始消费, 如果有一个分区存在没提交的offset, 则抛出异常, 不推荐
         *      2) latest: 当前消费组, 各分区offset都已提交时, 从提交的offset开始消费, 如果有一个分区存在没提交的offset, 则消费该分区新产生的数据(老数据不管), 默认
         *      3) earliest: 当前消费组, 各分区offset都已提交时, 从提交的offset开始消费, 如果有一个分区存在没提交的offset, 则从头开始消费, 不推荐
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(Const.TOPIC_CORE));
        System.err.println("core consumer1 started...");

        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition topicPartition : records.partitions()) {
                    String topic = topicPartition.topic();
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    int size = partitionRecords.size();

                    System.err.println(String.format("--- 获取topic: %s, 分区位置：%s, 消息总数： %s", topic, topicPartition.partition(), size));

                    for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
                        String value = consumerRecord.value();
                        long offset = consumerRecord.offset();
                        long commitOffset = offset + 1;
                        System.err.println(String.format("获取实际消息 value：%s, 消息offset: %s, 提交offset: %s", value, offset, commitOffset));
                    }
                }
            }
        } catch (Exception e){
            throw e;
        } finally {
            consumer.close();
        }
    }
}
