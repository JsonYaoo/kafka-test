package com.jsonyao.cs.api.consumer.core;

import com.jsonyao.cs.api.constant.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Consumer: 测试消费者订阅参数
 */
public class CoreConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "core-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        System.err.println("core consumer1 started...");

        /*1) 对于Consume消息的订阅subscribe方法: 可以订阅一个或者多个topic*/
//        consumer.subscribe(Collections.singletonList(Const.TOPIC_CORE));
        /*2) 对于Consume消息的订阅subscribe方法: 也可以支持正则表达式方式的订阅, 初次如果先执行Consumer会导致一开始找不到Topic的问题, 所以第一次Producer产生的消息会错过*/
//        consumer.subscribe(Pattern.compile("topic-.*"));
        /*3) 对于assign方法: 可以指定订阅某个主题下的某一个或者多个partition*/
//        consumer.assign(Arrays.asList(new TopicPartition(Const.TOPIC_CORE, 0), new TopicPartition(Const.TOPIC_CORE, 2)));
        /*4) 对于assign方法: 还拉取拉取主题下的所有partition*/
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(Const.TOPIC_CORE);
        for (PartitionInfo partitionInfo : partitionInfoList) {
            System.err.println("主题:"+ partitionInfo.topic() +", 分区: " + partitionInfo.partition());
            topicPartitionList.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        consumer.assign(topicPartitionList);

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
