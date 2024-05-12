package com.kafka.consumer.service;

import com.kafka.consumer.dto.MyCar;
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
public class MyCarConsumerService {

//    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 3000,multiplier = 1.5,maxDelay = 15000), exclude = {NullPointerException.class})
    @RetryableTopic(attempts = "4", kafkaTemplate = "kafkaTemplate")
    @KafkaListener(topics = TopicConstants.CAR_TOPIC, groupId = TopicConstants.CAR_GROUP_ID)
    public void consumeObjectV3(MyCar mycar, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {

            log.info("Consumed Car: {} :: topic:: {} offset:: {}", mycar,topic,offset);
            if(mycar.name().isBlank()){
                throw new RuntimeException("NO Car Name Provided");
            }

    }

    @DltHandler  //Dead Letter Topics
    public void listenDLT(MyCar mycar, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset){
        log.info("DLT MSG RECEIVED:: {},{},{}",mycar,topic,offset);
    }

}
