package com.kafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.dto.MyCar;
import com.kafka.consumer.dto.Person;
import com.kafka.consumer.util.TopicConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
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

//    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000,multiplier = 1.5,maxDelay = 15000), exclude = {NullPointerException.class})
    @RetryableTopic(attempts = "4")
    @KafkaListener(topics = TopicConstants.CAR_TOPIC, groupId = TopicConstants.CAR_GROUP_ID)
    public void consumeObjectV3(MyCar mycar, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            log.info("Consumed Car: {} :: topic:: {} offset:: {}", mycar,topic,offset);
            if(mycar.name().isBlank()){
                throw new RuntimeException("NO Car Name Provided");
            }

        }
        catch (RuntimeException e){
            log.error("ERR: {}",e.getLocalizedMessage());
        }
    }

    @DltHandler  //Dead Letter Topics
    public void listenDLT(MyCar mycar, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset){
        log.info("DLT MSG RECEIVED:: {},{},{}",mycar,topic,offset);
    }


}
