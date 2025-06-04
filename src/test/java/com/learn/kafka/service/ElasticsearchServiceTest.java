package com.learn.kafka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.RestClientException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElasticsearchServiceTest {

    @Mock
    private RestTemplate restTemplate;

    @InjectMocks
    private ElasticsearchService elasticsearchService;

    private ListAppender<ILoggingEvent> listAppender;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(elasticsearchService, "elasticsearchUrl", "http://localhost:9200");
        ReflectionTestUtils.setField(elasticsearchService, "restTemplate", restTemplate);

        // Configurer le logger pour capturer les logs
        Logger logger = (Logger) LoggerFactory.getLogger(ElasticsearchService.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @Test
    void shouldConsumeAndStoreInElasticsearchSuccessfully() {
        // Given
        String exchangeRateData = """
            {
                "base": "USD",
                "rates": {
                    "EUR": 0.85
                }
            }
            """;

        // Mock index exists check
        when(restTemplate.getForEntity(anyString(), eq(String.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));

        // Mock document storage
        when(restTemplate.exchange(anyString(), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.CREATED));

        // When
        elasticsearchService.consumeAndStoreInElasticsearch(exchangeRateData);

        // Then
        verify(restTemplate, times(1)).getForEntity(contains("/exchange-rates"), eq(String.class));
        verify(restTemplate, times(1)).exchange(
                contains("/exchange-rates/_doc/"),
                eq(HttpMethod.PUT),
                any(HttpEntity.class),
                eq(String.class)
        );

        // Vérifier les logs
        assertThat(listAppender.list).hasSize(2);
        assertThat(listAppender.list.get(0).getFormattedMessage())
                .contains("Consuming exchange rate data for Elasticsearch");
        assertThat(listAppender.list.get(1).getFormattedMessage())
                .contains("Exchange rate data stored in Elasticsearch with ID:");
    }

    @Test
    void shouldCreateIndexWhenItDoesNotExist() {
        // Given
        String exchangeRateData = "{}";

        // Mock index doesn't exist
        when(restTemplate.getForEntity(contains("/exchange-rates"), eq(String.class)))
                .thenThrow(new RestClientException("Index not found"));

        // Mock index creation and document storage
        when(restTemplate.exchange(anyString(), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));

        // When
        elasticsearchService.consumeAndStoreInElasticsearch(exchangeRateData);

        // Then
        verify(restTemplate, times(2)).exchange(
                anyString(),
                eq(HttpMethod.PUT),
                any(HttpEntity.class),
                eq(String.class)
        );

        // Vérifier que l'index a été créé
        assertThat(listAppender.list.stream()
                .anyMatch(event -> event.getFormattedMessage().contains("Created Elasticsearch index")))
                .isTrue();
    }

    @Test
    void shouldHandleElasticsearchErrorGracefully() {
        // Given
        String exchangeRateData = "{}";

        when(restTemplate.getForEntity(anyString(), eq(String.class)))
                .thenThrow(new RestClientException("Elasticsearch error"));

        when(restTemplate.exchange(anyString(), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class)))
                .thenThrow(new RestClientException("Storage error"));

        // When & Then (should not throw exception)
        elasticsearchService.consumeAndStoreInElasticsearch(exchangeRateData);

        // Verify error was logged
        assertThat(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.ERROR))
                .isTrue();
    }

    @Test
    void shouldHandleIndexCreationFailure() {
        // Given
        String exchangeRateData = "{}";

        // Index check fails
        when(restTemplate.getForEntity(contains("/exchange-rates"), eq(String.class)))
                .thenThrow(new RestClientException("Index not found"));

        // Index creation fails
        when(restTemplate.exchange(contains("/exchange-rates"), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class)))
                .thenThrow(new RestClientException("Creation failed"));

        // Document storage should still be attempted and fail
        when(restTemplate.exchange(contains("/_doc/"), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class)))
                .thenThrow(new RestClientException("Storage failed"));

        // When
        elasticsearchService.consumeAndStoreInElasticsearch(exchangeRateData);

        // Then
        verify(restTemplate, times(1)).getForEntity(contains("/exchange-rates"), eq(String.class));
        verify(restTemplate, atLeastOnce()).exchange(anyString(), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class));

        // Check warning log for index creation failure
        assertThat(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.WARN &&
                        event.getFormattedMessage().contains("Could not create/check Elasticsearch index")))
                .isTrue();
    }

    @Test
    void shouldUseCustomElasticsearchUrl() {
        // Given
        String customUrl = "http://custom-elasticsearch:9200";
        ReflectionTestUtils.setField(elasticsearchService, "elasticsearchUrl", customUrl);

        String exchangeRateData = "{}";

        when(restTemplate.getForEntity(anyString(), eq(String.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));
        when(restTemplate.exchange(anyString(), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.CREATED));

        // When
        elasticsearchService.consumeAndStoreInElasticsearch(exchangeRateData);

        // Then
        verify(restTemplate).getForEntity(eq(customUrl + "/exchange-rates"), eq(String.class));
        verify(restTemplate).exchange(
                startsWith(customUrl + "/exchange-rates/_doc/"),
                eq(HttpMethod.PUT),
                any(HttpEntity.class),
                eq(String.class)
        );
    }

    @Test
    void shouldHandleComplexExchangeRateData() {
        // Given
        String complexExchangeRateData = """
            {
                "timestamp": "2024-01-01T12:00:00.000Z",
                "base": "USD",
                "date": "2024-01-01",
                "rates": {
                    "EUR": 0.85,
                    "GBP": 0.75,
                    "JPY": 110.0,
                    "CAD": 1.25
                }
            }
            """;

        when(restTemplate.getForEntity(anyString(), eq(String.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.OK));
        when(restTemplate.exchange(anyString(), eq(HttpMethod.PUT), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>(HttpStatus.CREATED));

        // When
        elasticsearchService.consumeAndStoreInElasticsearch(complexExchangeRateData);

        // Then
        verify(restTemplate).exchange(
                anyString(),
                eq(HttpMethod.PUT),
                argThat(httpEntity -> {
                    String body = (String) httpEntity.getBody();
                    return body != null && body.contains("EUR") && body.contains("0.85");
                }),
                eq(String.class)
        );
    }
}