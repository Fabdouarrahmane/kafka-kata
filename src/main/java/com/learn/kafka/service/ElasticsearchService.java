package com.learn.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.UUID;

@Service
@Slf4j
public class ElasticsearchService {

    @Value("${elasticsearch.url:http://localhost:9200}")
    private String elasticsearchUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    @KafkaListener(topics = "exchange-rates", groupId = "elasticsearch-consumer")
    public void consumeAndStoreInElasticsearch(String exchangeRateData) {
        try {
            log.info("Consuming exchange rate data for Elasticsearch...");

            // Créer l'index s'il n'existe pas
            createIndexIfNotExists();

            // Stocker les données
            String documentId = UUID.randomUUID().toString();
            String url = elasticsearchUrl + "/exchange-rates/_doc/" + documentId;

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(exchangeRateData, headers);

            restTemplate.exchange(url, HttpMethod.PUT, request, String.class);

            log.info("Exchange rate data stored in Elasticsearch with ID: {}", documentId);

        } catch (Exception e) {
            log.error("Error storing data in Elasticsearch: {}", e.getMessage(), e);
        }
    }

    private void createIndexIfNotExists() {
        try {
            String indexUrl = elasticsearchUrl + "/exchange-rates";

            // Vérifier si l'index existe
            try {
                restTemplate.getForEntity(indexUrl, String.class);
            } catch (Exception e) {
                // L'index n'existe pas, le créer
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                String mapping = """
                {
                  "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                  },
                  "mappings": {
                    "properties": {
                      "timestamp": {
                        "type": "date",
                        "format": "yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd'T'HH:mm:ss||strict_date_optional_time"
                      },
                      "base": { "type": "keyword" },
                      "date": { "type": "date" },
                      "rates": { "type": "object" }
                    }
                  }
                }
                """;

                HttpEntity<String> request = new HttpEntity<>(mapping, headers);
                restTemplate.exchange(indexUrl, HttpMethod.PUT, request, String.class);
                log.info("Created Elasticsearch index: exchange-rates");
            }
        } catch (Exception e) {
            log.warn("Could not create/check Elasticsearch index: {}", e.getMessage());
        }
    }
}