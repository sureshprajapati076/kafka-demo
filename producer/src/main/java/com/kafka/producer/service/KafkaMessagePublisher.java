package com.kafka.producer.service;

import com.kafka.producer.dto.Person;
import com.kafka.producer.model.MyCar;
import com.kafka.producer.util.TopicConstants;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    private final KafkaTemplate<String,Object> kafkaTemplate;

    private final KafkaTemplate<String,Object> kafkaTemplate2;

    public KafkaMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate, @Qualifier("stringSerializer") KafkaTemplate<String, Object> kafkaTemplate2) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplate2 = kafkaTemplate2;
    }

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate2.send(TopicConstants.TEXT_ONLY_TOPIC, message);
        future.whenComplete((result,ex)->{
            if(null==ex) {
                System.out.println("SENT MESSAGE: " + message +
                        "with offset " + result.getRecordMetadata().offset()
                );
            }
            else{
                System.out.println(ex.getLocalizedMessage());
            }
        });

    }

    public void sendMessageToTopicV2(Person person) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TopicConstants.PERSON_TOPIC, person);
        future.whenComplete((result,ex)->{
            if(null==ex) {
                System.out.println("SENT MESSAGE: " + person +
                        "with offset " + result.getRecordMetadata().offset()
                );
            }
            else{
                System.out.println(ex.getLocalizedMessage());
            }
        });



    }

    public void sendMessageToTopicV3(MyCar myCar) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TopicConstants.CAR_TOPIC, myCar);
        future.whenComplete((result,ex)->{
            if(null==ex) {
                System.out.println("SENT MESSAGE: " + myCar +
                        "with offset " + result.getRecordMetadata().offset()
                );
            }
            else{
                System.out.println(ex.getLocalizedMessage());
            }
        });

    }
}
