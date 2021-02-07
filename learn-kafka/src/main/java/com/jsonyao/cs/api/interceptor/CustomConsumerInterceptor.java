package com.jsonyao.cs.api.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Kafka自定义消费者拦截器
 */
public class CustomConsumerInterceptor implements ConsumerInterceptor<String, String> {

    /**
     * 消费消息前拦截器
     * @param consumerRecords
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        System.err.println("------  消费者前置处理器，接收消息   --------");
        return consumerRecords;
    }

    /**
     * 消息消费完毕提交前拦截器: 默认配置了每5s轮训一次是否提交, 所有也会每5s执行该拦截器
     * @param map
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        map.forEach((tp, offSet) -> {
            System.err.println("消费者处理完成，" + "分区:" + tp + ", 偏移量：" + offSet);
        });
    }

    /**
     * 消费者关闭拦截器: 手工关闭的不会执行, 只有代码自动关闭的才会
     */
    @Override
    public void close() {
        System.err.println("----------- 消费者已关闭 ----------");
    }

    /**
     * 消费者初始化拦截器
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {
        System.err.println("----------- 消费者初始化完成 ----------");
    }
}
