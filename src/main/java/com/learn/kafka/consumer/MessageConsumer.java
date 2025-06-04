package com.learn.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageConsumer {

    @KafkaListener(topics = "mon-tunnel-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(String message) {
        log.info("Message receive : {}", message);
    }
}