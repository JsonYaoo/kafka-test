package com.jsonyao.cs.service;

/**
 * Kafka生产服务类
 */
public interface KafkaProducerService {

    /**
     * 发送消息
     * @param topic
     * @param data
     */
    void sendMessage(String topic, Object data);

}
