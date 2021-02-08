package com.jsonyao.cs.api.consumer.core;

import com.jsonyao.cs.api.constant.Const;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Consumer: 测试消费者手工提交方式
 */
public class CommitConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "core-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        /*1) 采用自动提交方式*/
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        /*2) 采用手工提交方式*/
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

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

                        /*5) 在Partition内一条消息做一次提交动作: 同步提交, 推荐, 可靠*/
//                        consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(commitOffset)));
                        /*6) 在Partition内一条消息做一次提交动作: 异步提交, 推荐, 可靠且高性能*/
                        consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(commitOffset)), new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                if(e != null) {
                                    System.err.println("error处理");
                                }
                                System.err.println("在Partition内一条消息做一次异步提交成功: " + map);
                            }
                        });
                    }

                    /*3) 一个Partition做一次提交动作: 同步提交, 不推荐*/
//                    consumer.commitSync();
                    /*4) 整体提交: 异步提交(线程非阻塞), 可回调可不回调, 不推荐*/
//                    consumer.commitAsync(new OffsetCommitCallback() {
//                        @Override
//                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
//                            if(e != null) {
//                                System.err.println("error处理");
//                            }
//                            System.err.println("一个Partition异步提交成功: " + map);
//                        }
//                    });
                }

                /*1) 整体提交: 同步提交(线程阻塞), 不推荐*/
//                consumer.commitSync();
                /*2) 整体提交: 异步提交(线程非阻塞), 可回调可不回调, 这里会轮训回调函数, 不推荐*/
//                consumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
//                        if(e != null) {
//                            System.err.println("error处理");
//                        }
//                        System.err.println("整体异步提交成功: " + map);
//                    }
//                });
            }
        } catch (Exception e){
            throw e;
        } finally {
            consumer.close();
        }
    }
}
