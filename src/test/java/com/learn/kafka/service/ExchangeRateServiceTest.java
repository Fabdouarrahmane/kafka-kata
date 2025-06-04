package com.learn.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.RestClientException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExchangeRateServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private RestTemplate restTemplate;

    @InjectMocks
    private ExchangeRateService exchangeRateService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private ListAppender<ILoggingEvent> listAppender;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(exchangeRateService, "restTemplate", restTemplate);
        ReflectionTestUtils.setField(exchangeRateService, "objectMapper", objectMapper);

        // Configurer le logger pour capturer les logs
        Logger logger = (Logger) LoggerFactory.getLogger(ExchangeRateService.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
    }

    @Test
    void shouldFetchAndPublishExchangeRatesSuccessfully() {
        // Given
        String mockApiResponse = """
            {
                "base": "USD",
                "date": "2024-01-01",
                "rates": {
                    "EUR": 0.85,
                    "GBP": 0.75
                }
            }
            """;

        ResponseEntity<String> responseEntity = new ResponseEntity<>(mockApiResponse, HttpStatus.OK);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(restTemplate, times(1)).getForEntity(anyString(), eq(String.class));
        verify(kafkaTemplate, times(1)).send(eq("exchange-rates"), any(String.class));

        // Vérifier les logs
        assertThat(listAppender.list).hasSize(2);
        assertThat(listAppender.list.get(0).getFormattedMessage())
                .contains("Fetching exchange rates from API");
        assertThat(listAppender.list.get(1).getFormattedMessage())
                .contains("Exchange rates published to Kafka topic");
    }

    @Test
    void shouldHandleApiErrorGracefully() {
        // Given
        when(restTemplate.getForEntity(anyString(), eq(String.class)))
                .thenThrow(new RestClientException("API Error"));

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(restTemplate, times(1)).getForEntity(anyString(), eq(String.class));
        verify(kafkaTemplate, never()).send(anyString(), anyString());

        // Vérifier le log d'erreur
        assertThat(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.ERROR))
                .isTrue();
    }

    @Test
    void shouldNotPublishWhenApiResponseIsNotSuccessful() {
        // Given
        ResponseEntity<String> responseEntity = new ResponseEntity<>("Error", HttpStatus.INTERNAL_SERVER_ERROR);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(restTemplate, times(1)).getForEntity(anyString(), eq(String.class));
        verify(kafkaTemplate, never()).send(anyString(), anyString());

        // Vérifier qu'il n'y a que le log de fetch, pas de publication
        assertThat(listAppender.list).hasSize(1);
        assertThat(listAppender.list.getFirst().getFormattedMessage())
                .contains("Fetching exchange rates from API");
    }

    @Test
    void shouldAddTimestampToResponseData() {
        // Given
        String mockApiResponse = """
            {
                "base": "USD",
                "date": "2024-01-01",
                "rates": {
                    "EUR": 0.85
                }
            }
            """;

        ResponseEntity<String> responseEntity = new ResponseEntity<>(mockApiResponse, HttpStatus.OK);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(kafkaTemplate).send(eq("exchange-rates"), argThat(jsonData -> {
            try {
                return objectMapper.readTree(jsonData).has("timestamp");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    @Test
    void shouldHandleMalformedJsonResponse() {
        // Given
        String malformedJson = "{ invalid json }";
        ResponseEntity<String> responseEntity = new ResponseEntity<>(malformedJson, HttpStatus.OK);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(restTemplate, times(1)).getForEntity(anyString(), eq(String.class));
        verify(kafkaTemplate, never()).send(anyString(), anyString());

        // Vérifier le log d'erreur
        assertThat(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.ERROR))
                .isTrue();
    }

    @Test
    void shouldHandleNullResponseBody() {
        // Given
        ResponseEntity<String> responseEntity = new ResponseEntity<>(null, HttpStatus.OK);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(restTemplate, times(1)).getForEntity(anyString(), eq(String.class));
        verify(kafkaTemplate, never()).send(anyString(), anyString());

        // Vérifier le log d'erreur
        assertThat(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.ERROR))
                .isTrue();
    }

    @Test
    void shouldHandleKafkaPublishingError() {
        // Given
        String mockApiResponse = """
            {
                "base": "USD",
                "rates": {
                    "EUR": 0.85
                }
            }
            """;

        ResponseEntity<String> responseEntity = new ResponseEntity<>(mockApiResponse, HttpStatus.OK);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);
        doThrow(new RuntimeException("Kafka error")).when(kafkaTemplate).send(anyString(), anyString());

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(restTemplate, times(1)).getForEntity(anyString(), eq(String.class));
        verify(kafkaTemplate, times(1)).send(eq("exchange-rates"), any(String.class));

        // Vérifier le log d'erreur
        assertThat(listAppender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.ERROR))
                .isTrue();
    }

    @Test
    void shouldUseCorrectApiUrl() {
        // Given
        String mockApiResponse = "{}";
        ResponseEntity<String> responseEntity = new ResponseEntity<>(mockApiResponse, HttpStatus.OK);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(restTemplate).getForEntity(
                eq("https://api.exchangerate-api.com/v4/latest/USD"),
                eq(String.class)
        );
    }

    @Test
    void shouldUseCorrectKafkaTopic() {
        // Given
        String mockApiResponse = """
            {
                "base": "USD",
                "rates": {
                    "EUR": 0.85
                }
            }
            """;

        ResponseEntity<String> responseEntity = new ResponseEntity<>(mockApiResponse, HttpStatus.OK);
        when(restTemplate.getForEntity(anyString(), eq(String.class))).thenReturn(responseEntity);

        // When
        exchangeRateService.fetchAndPublishExchangeRates();

        // Then
        verify(kafkaTemplate).send(eq("exchange-rates"), any(String.class));
    }
}