package com.learn.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigTest {

    private KafkaConsumerConfig kafkaConsumerConfig;

    @BeforeEach
    void setUp() {
        kafkaConsumerConfig = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(kafkaConsumerConfig, "bootstrapServers", "localhost:9092");
        ReflectionTestUtils.setField(kafkaConsumerConfig, "consumerGroupId", "test-group");
    }

    @Test
    void shouldCreateConsumerFactoryWithCorrectConfiguration() {
        // When
        ConsumerFactory<String, String> consumerFactory = kafkaConsumerConfig.consumerFactory();

        // Then
        assertNotNull(consumerFactory);
        assertThat(consumerFactory).isInstanceOf(DefaultKafkaConsumerFactory.class);

        // Vérifier la configuration
        DefaultKafkaConsumerFactory<String, String> defaultFactory =
                (DefaultKafkaConsumerFactory<String, String>) consumerFactory;

        Map<String, Object> configProps = defaultFactory.getConfigurationProperties();

        assertThat(configProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        assertThat(configProps.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("test-group");
        assertThat(configProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(configProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
    }

    @Test
    void shouldCreateKafkaListenerContainerFactory() {
        // When
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                kafkaConsumerConfig.kafkaListenerContainerFactory();

        // Then
        assertNotNull(factory);
        assertNotNull(factory.getConsumerFactory());
    }

    @Test
    void shouldUseInjectedBootstrapServers() {
        // Given
        String customBootstrapServers = "custom-server:9092";
        ReflectionTestUtils.setField(kafkaConsumerConfig, "bootstrapServers", customBootstrapServers);

        // When
        ConsumerFactory<String, String> consumerFactory = kafkaConsumerConfig.consumerFactory();

        // Then
        DefaultKafkaConsumerFactory<String, String> defaultFactory =
                (DefaultKafkaConsumerFactory<String, String>) consumerFactory;
        Map<String, Object> configProps = defaultFactory.getConfigurationProperties();

        assertThat(configProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo(customBootstrapServers);
    }

    @Test
    void shouldUseInjectedConsumerGroupId() {
        // Given
        String customGroupId = "custom-group-id";
        ReflectionTestUtils.setField(kafkaConsumerConfig, "consumerGroupId", customGroupId);

        // When
        ConsumerFactory<String, String> consumerFactory = kafkaConsumerConfig.consumerFactory();

        // Then
        DefaultKafkaConsumerFactory<String, String> defaultFactory =
                (DefaultKafkaConsumerFactory<String, String>) consumerFactory;
        Map<String, Object> configProps = defaultFactory.getConfigurationProperties();

        assertThat(configProps.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo(customGroupId);
    }
}