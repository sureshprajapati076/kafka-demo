package com.kafka.producer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.dto.Person;
import com.kafka.producer.model.MyCar;
import com.kafka.producer.service.KafkaMessagePublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {
    private final KafkaMessagePublisher kafkaMessagePublisher;

    private final ObjectMapper objectMapper;

    public ProducerController(KafkaMessagePublisher kafkaMessagePublisher, ObjectMapper objectMapper) {
        this.kafkaMessagePublisher = kafkaMessagePublisher;
        this.objectMapper = objectMapper;
    }

//    @PostMapping("/produce")
//    public ResponseEntity<?> produce(@RequestBody String message){
//        try {
//            for(int i=0;i<10000;i++) {
//                kafkaMessagePublisher.sendMessageToTopic(message+" "+i);
//            }
//            return ResponseEntity.ok("MEssage Delivered");
//        }catch (Exception ex){
//            return ResponseEntity.status(500).body("ERROR");
//        }
//    }

//    @PostMapping("/post/v1")
//    public ResponseEntity<?> producev1(@RequestBody Person person){
//        try {
//            var message = objectMapper.writeValueAsString(person);
//            kafkaMessagePublisher.sendMessageToTopic(message);
//            return ResponseEntity.ok("Message Delivered");
//        }catch (Exception ex){
//            return ResponseEntity.status(500).body("ERROR");
//        }
//    }

    @PostMapping("/post/v2")
    public ResponseEntity<?> producev2(@RequestBody Person person){
        try {
            kafkaMessagePublisher.sendMessageToTopicV2(person);
            return ResponseEntity.ok("Message Delivered");
        }catch (Exception ex){
            System.out.println(ex.getLocalizedMessage());
            return ResponseEntity.status(500).body("ERROR");
        }
    }

    @PostMapping("/post/v3")
    public ResponseEntity<?> producev3(@RequestBody MyCar car){
        try {
            kafkaMessagePublisher.sendMessageToTopicV3(car);
            return ResponseEntity.ok("Message Delivered");
        }catch (Exception ex){
            System.out.println(ex.getLocalizedMessage());
            return ResponseEntity.status(500).body("ERROR");
        }
    }

}
