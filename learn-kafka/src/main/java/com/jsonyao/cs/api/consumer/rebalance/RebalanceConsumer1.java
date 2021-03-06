package com.jsonyao.cs.api.consumer.rebalance;

import com.jsonyao.cs.api.constant.Const;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Consumer: 测试同组消费者再均衡
 */
public class RebalanceConsumer1 {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "rebalance-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        /*测试同组消费者再均衡*/
        consumer.subscribe(Collections.singletonList(Const.TOPIC_REBALANCE), new ConsumerRebalanceListener() {

            /**
             * 撤销/回收已分配的消费者
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.err.println("Revoked Partitions:" + partitions);
            }

            /**
             * 分配消费者
             * @param partitions
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.err.println("Assigned Partitions:" + partitions);
            }
        });
        System.err.println("rebalance consumer1 started...");

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
