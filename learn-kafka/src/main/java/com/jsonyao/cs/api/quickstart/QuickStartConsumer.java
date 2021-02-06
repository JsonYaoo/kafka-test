package com.jsonyao.cs.api.quickstart;

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
 * Kafka Consumer急速入门测试类
 */
public class QuickStartConsumer {

    public static void main(String[] args) {
        // 1. 配置属性参数
        Properties properties = new Properties();
        /*1.1. 连接kafka集群的服务列表，如果有多个，使用逗号进行分隔*/
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        /*1.2. 设置订阅组ID, 与消费者订阅组有关系*/
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "quickstart-group");
        /*1.3. Key反序列化器*/
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /*1.4. Value反序列化器*/
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /*1.5. 设置常规属性: 会话连接超时时间*/
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        /*1.6. 设置常规属性: 自动提交与自动提交周期, 默认不用设置*/
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        // 2. 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3. 订阅感兴趣的主题
        consumer.subscribe(Collections.singletonList(Const.TOPIC_QUICKSTART));
        System.err.println("quickstart consumer started...");

        /*监听消息*/
        try {
            while (true){
                // 4. 采用拉取的方式消费数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                /*遍历所有Partition*/
                for (TopicPartition topicPartition : records.partitions()) {
                    /*获取该partition对应的topic*/
                    String topic = topicPartition.topic();
                    /*获取该partition下的所有消息*/
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    int size = partitionRecords.size();

                    System.err.println(String.format("--- 获取topic: %s, 分区位置：%s, 消息总数： %s", topic, topicPartition.partition(), size));

                    /*遍历当前partition下的所有消息*/
                    for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
                        /*实际消息内容*/
                        String value = consumerRecord.value();
                        /*消息偏移量*/
                        long offset = consumerRecord.offset();
                        /*当前消息提交的offset: 表示下一次从什么位置开始拉取消息*/
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
