package com.kafka.consumer.service;

import com.kafka.consumer.dto.MyCar;
import com.kafka.consumer.dto.Person;
import com.kafka.consumer.util.TopicConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {

//    @KafkaListener(topics = "sample123")
//    public void consumeObject(String message) throws JsonProcessingException {
//        var result = objectMapper.readValue(message, Person.class);
//        log.info("Consumed V1: {}",result);
//    }

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

//    @KafkaListener(topics = "demo0", groupId = "g-id")
//    public void consume1(String message){
//        log.info("Consumed 1: {}",message);
//    }



//    @KafkaListener(topics = "praja")
//    public void consume2(String message){
//        log.info("Consumed 2: {}",message);
//    }
//    @KafkaListener(topics = "praja")
//    public void consume3(String message){
//        log.info("Consumed 3: {}",message);
//    }
//
//    @KafkaListener(topics = "praja")
//    public void consume4(String message){
//        log.info("Consumed 4: {}",message);
//    }


}
