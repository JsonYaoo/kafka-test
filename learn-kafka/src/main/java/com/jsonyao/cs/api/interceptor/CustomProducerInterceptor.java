package com.jsonyao.cs.api.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka自定义Producer拦截器
 */
public class CustomProducerInterceptor implements ProducerInterceptor<String, String> {

    /**
     * 成功消息统计
     */
    private AtomicInteger success = new AtomicInteger(0);

    /**
     * 失败消息统计
     */
    private AtomicInteger failure = new AtomicInteger(0);

    /**
     * 消息发送前置拦截器
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        System.err.println("----------- 生产者发送消息前置拦截器 ----------");
        String modifiedValue = "prefix-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), modifiedValue, record.headers());
    }

    /**
     * 消息发送后置拦截器
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.err.println("----------- 生产者发送消息后置拦截器 ----------");
        if(e == null){
            success.getAndIncrement();
        }else {
            failure.getAndIncrement();
        }
    }

    /**
     * 关闭拦截器
     */
    @Override
    public void close() {
        int successSum = success.get();
        int failureSum = failure.get();
        int sum = successSum + failureSum;

        double successRatio = 0.0000D;
        if(sum != 0){
            successRatio = successSum / sum;
        }
        System.err.println(String.format("生产者关闭，发送消息的成功率为：%s %%", successRatio * 100));
    }

    /**
     * 初始化拦截器
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
