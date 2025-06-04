package com.learn.kafka.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class MessageProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private MessageProducer messageProducer;

    @Test
    void shouldSendMessageToKafka() {
        // Given
        String topic = "test-topic";
        String message = "test message";

        // When
        messageProducer.sendMessage(topic, message);

        // Then
        verify(kafkaTemplate, times(1)).send(topic, message);
    }
}