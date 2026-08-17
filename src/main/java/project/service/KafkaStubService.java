package project.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaStubService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaStubService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "input-topic", groupId = "stub-group", concurrency = "2")
    public void process(String message) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(message);

        // Берем ID из входящего сообщения
        String id = jsonNode.get("id").asText();

        // Модифицируем
        ((ObjectNode) jsonNode).put("id", id + "У123Ф");
        String outputMessage = objectMapper.writeValueAsString(jsonNode);

        // Отправляем с ключом. Ключ — залог распределения.
        kafkaTemplate.send("output-topic", id, outputMessage);
    }
}