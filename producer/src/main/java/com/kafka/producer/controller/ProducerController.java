package com.kafka.producer.controller;

import com.kafka.producer.dto.Person;
import com.kafka.producer.model.MyCar;
import com.kafka.producer.service.KafkaMessagePublisher;
import com.kafka.producer.util.TopicConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@Slf4j
public class ProducerController {
    private final KafkaMessagePublisher kafkaMessagePublisher;

    private final ObjectMapper objectMapper;

    private final KafkaTemplate<String,Object> kafkaTemplate;

    public ProducerController(KafkaMessagePublisher kafkaMessagePublisher, ObjectMapper objectMapper, KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaMessagePublisher = kafkaMessagePublisher;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/send")
    public ResponseEntity<?> ok(@RequestBody Map<String,Object> objectMap){

        kafkaTemplate.send(TopicConstants.OBJ, objectMap)
                .thenAccept(result -> {
                    // Extract metadata from the result
                    var metadata = result.getRecordMetadata();
                    log.info("Message sent successfully! Topic: {}, Partition: {}, Offset: {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                })
                .exceptionally(ex -> {
                    log.error("Failed to send message", ex);
                    return null;
                });

        return ResponseEntity.ok("OK");
    }

    @PostMapping("/produce")
    public ResponseEntity<?> produce(@RequestBody String message){
        try {
            for(int i=0;i<10;i++) {
                kafkaMessagePublisher.sendMessageToTopic(message+" "+i);
            }
            return ResponseEntity.ok("Message Delivered");
        }catch (Exception ex){
            return ResponseEntity.status(500).body("ERROR");
        }
    }

    @PostMapping("/post/v1")
    public ResponseEntity<?> producev1(@RequestBody Person person){
        try {
            var message = objectMapper.writeValueAsString(person);
            kafkaMessagePublisher.sendMessageToTopic(message);
            return ResponseEntity.ok("Message Delivered");
        }catch (Exception ex){
            return ResponseEntity.status(500).body("ERROR");
        }
    }

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
