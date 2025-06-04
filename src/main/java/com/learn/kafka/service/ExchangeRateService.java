package com.learn.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Service
@Slf4j
public class ExchangeRateService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String API_URL = "https://api.exchangerate-api.com/v4/latest/USD";
    private static final String KAFKA_TOPIC = "exchange-rates";

    @Scheduled(fixedRate = 30000) // Toutes les 30 secondes
    public void fetchAndPublishExchangeRates() {
        try {
            log.info("Fetching exchange rates from API...");

            ResponseEntity<String> response = restTemplate.getForEntity(API_URL, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                String rateData = response.getBody();

                // Ajouter timestamp pour Elasticsearch avec timezone
                JsonNode jsonNode = objectMapper.readTree(rateData);
                String timestamp = LocalDateTime.now().atZone(java.time.ZoneId.systemDefault()).toInstant().toString();
                ((com.fasterxml.jackson.databind.node.ObjectNode) jsonNode).put("timestamp", timestamp);

                String enrichedData = objectMapper.writeValueAsString(jsonNode);

                // Publier sur Kafka
                kafkaTemplate.send(KAFKA_TOPIC, enrichedData);
                log.info("Exchange rates published to Kafka topic: {}", KAFKA_TOPIC);
            }
        } catch (Exception e) {
            log.error("Error fetching/publishing exchange rates: {}", e.getMessage(), e);
        }
    }
}