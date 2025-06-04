package com.learn.kafka.consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class MessageConsumerTest {

    @InjectMocks
    private MessageConsumer messageConsumer;

    @Test
    void shouldLogReceivedMessage() {
        // Given
        String testMessage = "Test message for Kafka consumer";

        // Configurer un appender pour capturer les logs
        Logger logger = (Logger) LoggerFactory.getLogger(MessageConsumer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        // When
        messageConsumer.listen(testMessage);

        // Then
        assertThat(listAppender.list).hasSize(1);
        ILoggingEvent logEvent = listAppender.list.getFirst();
        assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
        assertThat(logEvent.getFormattedMessage()).contains("Message receive : " + testMessage);

        // Nettoyer
        logger.detachAppender(listAppender);
    }

    @Test
    void shouldHandleNullMessage() {
        // Given
        Logger logger = (Logger) LoggerFactory.getLogger(MessageConsumer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        // When
        messageConsumer.listen(null);

        // Then
        assertThat(listAppender.list).hasSize(1);
        ILoggingEvent logEvent = listAppender.list.getFirst();
        assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
        assertThat(logEvent.getFormattedMessage()).contains("Message receive : null");

        // Nettoyer
        logger.detachAppender(listAppender);
    }

    @Test
    void shouldHandleEmptyMessage() {
        // Given
        String emptyMessage = "";
        Logger logger = (Logger) LoggerFactory.getLogger(MessageConsumer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        // When
        messageConsumer.listen(emptyMessage);

        // Then
        assertThat(listAppender.list).hasSize(1);
        ILoggingEvent logEvent = listAppender.list.getFirst();
        assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
        assertThat(logEvent.getFormattedMessage()).contains("Message receive : ");

        // Nettoyer
        logger.detachAppender(listAppender);
    }

    @Test
    void shouldHandleLongMessage() {
        // Given
        String longMessage = "A".repeat(1000); // Message de 1000 caractères
        Logger logger = (Logger) LoggerFactory.getLogger(MessageConsumer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        // When
        messageConsumer.listen(longMessage);

        // Then
        assertThat(listAppender.list).hasSize(1);
        ILoggingEvent logEvent = listAppender.list.getFirst();
        assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
        assertThat(logEvent.getFormattedMessage()).contains("Message receive : " + longMessage);

        // Nettoyer
        logger.detachAppender(listAppender);
    }

    @Test
    void shouldHandleJsonMessage() {
        // Given
        String jsonMessage = "{\"key\":\"value\",\"number\":123,\"boolean\":true}";
        Logger logger = (Logger) LoggerFactory.getLogger(MessageConsumer.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        // When
        messageConsumer.listen(jsonMessage);

        // Then
        assertThat(listAppender.list).hasSize(1);
        ILoggingEvent logEvent = listAppender.list.getFirst();
        assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
        assertThat(logEvent.getFormattedMessage()).contains("Message receive : " + jsonMessage);

        // Nettoyer
        logger.detachAppender(listAppender);
    }
}