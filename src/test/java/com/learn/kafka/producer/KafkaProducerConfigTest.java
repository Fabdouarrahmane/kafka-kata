package com.learn.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class KafkaProducerConfigTest {

    private KafkaProducerConfig kafkaProducerConfig;

    @BeforeEach
    void setUp() {
        kafkaProducerConfig = new KafkaProducerConfig();
        ReflectionTestUtils.setField(kafkaProducerConfig, "bootstrapServers", "localhost:9092");
    }

    @Test
    void shouldCreateProducerFactoryWithCorrectConfiguration() {
        // When
        ProducerFactory<String, String> producerFactory = kafkaProducerConfig.producerFactory();

        // Then
        assertNotNull(producerFactory);
        assertThat(producerFactory).isInstanceOf(DefaultKafkaProducerFactory.class);

        // Vérifier la configuration
        DefaultKafkaProducerFactory<String, String> defaultFactory =
                (DefaultKafkaProducerFactory<String, String>) producerFactory;

        Map<String, Object> configProps = defaultFactory.getConfigurationProperties();

        assertThat(configProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        assertThat(configProps.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(configProps.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
    }

    @Test
    void shouldCreateKafkaTemplate() {
        // When
        KafkaTemplate<String, String> kafkaTemplate = kafkaProducerConfig.kafkaTemplate();

        // Then
        assertNotNull(kafkaTemplate);
        assertNotNull(kafkaTemplate.getProducerFactory());
    }

    @Test
    void shouldUseInjectedBootstrapServers() {
        // Given
        String customBootstrapServers = "custom-server:9092";
        ReflectionTestUtils.setField(kafkaProducerConfig, "bootstrapServers", customBootstrapServers);

        // When
        ProducerFactory<String, String> producerFactory = kafkaProducerConfig.producerFactory();

        // Then
        DefaultKafkaProducerFactory<String, String> defaultFactory =
                (DefaultKafkaProducerFactory<String, String>) producerFactory;
        Map<String, Object> configProps = defaultFactory.getConfigurationProperties();

        assertThat(configProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo(customBootstrapServers);
    }

    @Test
    void shouldKafkaTemplateUseCorrectProducerFactory() {
        // Given
        ProducerFactory<String, String> expectedProducerFactory = kafkaProducerConfig.producerFactory();

        // When
        KafkaTemplate<String, String> kafkaTemplate = kafkaProducerConfig.kafkaTemplate();

        // Then
        assertNotNull(kafkaTemplate.getProducerFactory());
        // Vérifier que les configurations sont identiques
        DefaultKafkaProducerFactory<String, String> templateFactory =
                (DefaultKafkaProducerFactory<String, String>) kafkaTemplate.getProducerFactory();
        DefaultKafkaProducerFactory<String, String> expectedFactory =
                (DefaultKafkaProducerFactory<String, String>) expectedProducerFactory;

        assertThat(templateFactory.getConfigurationProperties())
                .containsAllEntriesOf(expectedFactory.getConfigurationProperties());
    }
}