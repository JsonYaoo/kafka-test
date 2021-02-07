package com.jsonyao.cs.api.producer;

import com.alibaba.fastjson.JSON;
import com.jsonyao.cs.api.constant.Const;
import com.jsonyao.cs.api.entity.User;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Kafka测试生产者
 */
public class NormalProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.111:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "normal-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /**
         * 1.5 消息重试机制: 重试次数(默认是0)与重试间隔?
         *      1) 可重试异常: 意思是执行指定的重试次数, 如果到达重试次数上限还没有发送成功, 也会抛出异常信息
         *          eg: NetworkException、LeaderNotAvailableException
         *      2) 不可重试异常: 发生则不会继续重试
         *          eg: RecordTooLargeException
         */
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        User user = new User("001", "xiao xiao");
        ProducerRecord<String, String> record = new ProducerRecord<>(Const.TOPIC_NORMAL, JSON.toJSONString(user));

        /**
         * ProducerRecord(
         *      topic=topic-normal,
         *      partition=null,
         *      headers=RecordHeaders(headers = [], isReadOnly = false),
         *      key=null,
         *      value={"id":"001","name":"xiao xiao"},
         *      timestamp=null
         * )
         * 而一条消息必须通过key 去计算出来实际的partition, 按照partition去存储的, 所以key为空时Send方法会计算出partition
         */
        System.err.println("新创建消息：" + record);

        // 4. 发送消息
        /**
         * 4.1) 一个参数的send方法: 本质上也是异步的, 返回的是一个future对象, 但调用get()时为同步阻塞方式
         *     分区：3, 偏移量: 2, 时间戳: 1612621554999
         */
//        Future<RecordMetadata> future = producer.send(record);
//        RecordMetadata recordMetadata = future.get();
//        System.err.println(String.format("分区：%s, 偏移量: %s, 时间戳: %s", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()));
        /**
         * 4.2) 带有两个参数的send方法: 是完全异步化的, 在回调Callback方法中得到发送消息的结果
         *      分区：3, 偏移量: 0, 时间戳: 1612621997593
         */
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    e.printStackTrace();
                    return;
                }
                System.err.println(String.format("分区：%s, 偏移量: %s, 时间戳: %s", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()));
            }
        });

        System.err.println("quickstart producer send....");
        producer.close();
    }
}
