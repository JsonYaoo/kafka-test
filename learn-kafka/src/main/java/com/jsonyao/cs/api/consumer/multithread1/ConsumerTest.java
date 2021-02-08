package com.jsonyao.cs.api.consumer.multithread1;

import com.jsonyao.cs.api.constant.Const;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Kafka Consumer测试类: 测试一Consumer一Partition多线程模型
 */
public class ConsumerTest {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "module-group-id-1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        /**
         * 测试消费者多线程模型: 一个线程一个Consumer多线程模型是线程安全的, 因为一个Consumer只消费一个Partition, 而当多个线程共用同一个Consumer会报ConcurrentModificationException
         */
        int coreSize = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(coreSize);
        for(int i = 0; i < coreSize; i++){
            executorService.execute(new KafkaConsumerMt1(properties, Const.TOPIC_MT1));
        }
    }
}
