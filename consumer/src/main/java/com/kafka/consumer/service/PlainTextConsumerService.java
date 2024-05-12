package com.kafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.dto.Person;
import com.kafka.consumer.util.TopicConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PlainTextConsumerService {

    private final ObjectMapper objectMapper;

    public PlainTextConsumerService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    //    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000,multiplier = 1.5,maxDelay = 15000), exclude = {NullPointerException.class})
    @RetryableTopic(attempts = "4", kafkaTemplate = "stringSerializer")
    @KafkaListener(topics = TopicConstants.TEXT_ONLY_TOPIC, groupId = TopicConstants.TEXT_ONLY_GROUP_ID, containerFactory = "stringDeserializer")
    public void consumeObject(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
            try {
                var result = objectMapper.readValue(message, Person.class);
                log.info("Consumed as Person Object: {}", result);
                if (message.contains("ERR")) throw new RuntimeException("ERR");
            } catch (JsonProcessingException ex) {
                log.info("Consumed as plain text: {}", message);
                if (message.contains("ERR")) throw new RuntimeException("ERR");
            }
    }

    @DltHandler  //Dead Letter Topics
    public void listenDLT(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset){
        log.info("DLT MSG RECEIVED:: {},{},{}",message,topic,offset);
    }

}
