package com.learn.kafka;

import com.learn.kafka.producer.MessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(ProducerController.class)
class ProducerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MessageProducer messageProducer;

    @Test
    void shouldSendMessageAndReturnOk() throws Exception {
        // Given
        String content = "test message";

        // When & Then
        mockMvc.perform(post("/produce")
                        .param("content", content))
                .andExpect(status().isOk())
                .andExpect(content().string(content));

        verify(messageProducer, times(1)).sendMessage("mon-tunnel-topic", content);
    }
}