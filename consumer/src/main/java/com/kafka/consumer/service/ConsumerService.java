package com.kafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.dto.MyCar;
import com.kafka.consumer.dto.Person;
import com.kafka.consumer.util.TopicConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {

    private final ObjectMapper objectMapper;

    public ConsumerService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = TopicConstants.TEXT_ONLY_TOPIC, groupId = TopicConstants.TEXT_ONLY_GROUP_ID, containerFactory = "stringDeserializer")
    public void consumeObject(String message) {
        try{
            var result = objectMapper.readValue(message, Person.class);
            log.info("Consumed as Person Object: {}",result);
        }
        catch (JsonProcessingException ex){
            log.info("Consumed as plain text: {}",message);
        }

    }

    @KafkaListener(topics = TopicConstants.PERSON_TOPIC, groupId = TopicConstants.PERSON_GROUP_ID)
    public void consumeObjectV2(Person person) {
        System.out.println("Person CONSUMED....");
        log.info("Consumed V2: {}",person);
    }

    @KafkaListener(topics = TopicConstants.CAR_TOPIC, groupId = TopicConstants.CAR_GROUP_ID)
    public void consumeObjectV3(MyCar mycar) {
        System.out.println("Car CONSUMED....");
        log.info("Consumed V3: {}",mycar);
    }

}
