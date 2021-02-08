package com.jsonyao.cs.service.impl;

import com.jsonyao.cs.service.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
@Slf4j
public class KafkaProducerServiceImpl implements KafkaProducerService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void sendMessage(String topic, Object data) {
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, data);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            /**
             * 失败回调
             * @param throwable
             */
            @Override
            public void onFailure(Throwable throwable) {
                log.error("发送消息失败: " + throwable.getMessage());

            }

            /**
             * 成功回调
             * @param result
             */
            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("发送消息成功: " + result.toString());
            }
        });
    }

}
