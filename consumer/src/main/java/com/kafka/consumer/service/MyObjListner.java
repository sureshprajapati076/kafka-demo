package com.kafka.consumer.service;

import com.kafka.consumer.dto.Person;
import com.kafka.consumer.util.TopicConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaMessageHeaderAccessor;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Map;


@Service
@Slf4j
public class MyObjListner {

        @RetryableTopic(attempts = "4",kafkaTemplate = "kafkaTemplate", backOff = @BackOff(delay = 5000,multiplier = 2,maxDelay = 150000), exclude = {NullPointerException.class})
   // @RetryableTopic(attempts = "4", kafkaTemplate = "kafkaTemplate")
    @KafkaListener(topics = TopicConstants.OBJ, groupId = TopicConstants.OBJ_GRP)
    public void consumeObjectV2(Map<String,Object> objectMap, KafkaMessageHeaderAccessor accessor, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset,@Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.info("Consumed V2: {}",objectMap);
            int currentAttempt = accessor.getNonBlockingRetryDeliveryAttempt();
            log.warn("ATTEMPT: {} topic: {} offset: {} partition: {}",currentAttempt, topic, offset, partition);
        if(objectMap.containsKey("hello")){
            throw new RuntimeException("EXP");
        }
    }


    @DltHandler  //Dead Letter Topics
    public void listenDLT(Map<String,Object> objectMap, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset){
        log.info("DLT MSG RECEIVED:: {},{},{}",objectMap,topic,offset);
    }

}
