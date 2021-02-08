package com.jsonyao.cs.api.consumer.multithread1;

import com.jsonyao.cs.api.constant.Const;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka Consumer实体: 测试一Consumer一Partition多线程模型
 */
public class KafkaConsumerMt1 implements Runnable{

    /**
     * Kafka名称
     */
    private String consumerName;

    /**
     * Consumer实体
     */
    private KafkaConsumer<String, String> consumer;

    /**
     * Consumer线程组是否在运行
     */
    private volatile boolean isRunning = true;

    /**
     * Consumer计数器
     */
    private static AtomicInteger counter = new AtomicInteger(0);

    public KafkaConsumerMt1(Properties properties, String topic) {
        this.consumerName = "KafkaConsumerMt1-" + counter.getAndIncrement();
        this.consumer = new KafkaConsumer<>(properties);

        /*查看同组消费者会再均衡情况*/
        this.consumer.subscribe(Arrays.asList(Const.TOPIC_MT1), new ConsumerRebalanceListener() {

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

        System.err.println(this.consumerName + " started ");
    }

    /**
     * 执行Consumer线程监听方法
     */
    @Override
    public void run() {
        try {
            while (isRunning){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (TopicPartition topicPartition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
                        String value = consumerRecord.value();
                        long offset = consumerRecord.offset();
                        System.err.println("当前消费者: "+ consumerName + ",消息内容：" + value + ", 消息的偏移量: " + offset + "当前线程：" + Thread.currentThread().getName());
                    }
                }
            }
        } catch (Exception e){
            throw e;
        } finally {
            consumer.close();
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
}
