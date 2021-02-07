package com.jsonyao.cs.api.partition;

import com.jsonyao.cs.api.constant.Const;
import com.jsonyao.cs.api.entity.User;
import com.jsonyao.cs.api.interceptor.CustomConsumerInterceptor;
import com.jsonyao.cs.api.serial.UserDeserializer;
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
 * Kafka Consumer测试类: 自定义Producer Partition
 */
public class PartitionConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "partition-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class.getName());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        /**
         * 添加消费端拦截器属性: 可以配置多个拦截器
         */
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomConsumerInterceptor.class.getName());

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(Const.TOPIC_PARTITION));
        System.err.println("partition consumer started...");

        try {
            while (true){
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition topicPartition : records.partitions()) {
                    String topic = topicPartition.topic();
                    List<ConsumerRecord<String, User>> partitionRecords = records.records(topicPartition);
                    int size = partitionRecords.size();

                    System.err.println(String.format("--- 获取topic: %s, 分区位置：%s, 消息总数： %s", topic, topicPartition.partition(), size));

                    for (ConsumerRecord<String, User> consumerRecord : partitionRecords) {
                        User value = consumerRecord.value();
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
