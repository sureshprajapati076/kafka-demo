package com.kafka.consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String,Object>> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String,Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    public ConsumerFactory<String,Object> consumerFactory(){
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    public Map<String,Object> consumerConfig(){
        Map<String,Object> props =  new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);


        // Solution for error: This error handler cannot process 'SerializationException's directly; please consider configuring an 'ErrorHandlingDeserializer'
        // link: https://stackoverflow.com/questions/70252047/spring-kafka-error-this-error-handler-cannot-process-serializationexceptions

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);



        // Below config is used to map from producer dto to consumer dto in terms of packages...
        props.put(JsonDeserializer.TYPE_MAPPINGS,"com.kafka.producer.dto.Person:com.kafka.consumer.dto.Person,com.kafka.producer.model.MyCar:com.kafka.consumer.dto.MyCar");
        props.put(JsonDeserializer.TRUSTED_PACKAGES,"*");
        return props;
    }

    @Bean(name = "stringDeserializer")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String,Object>> kafkaListenerContainerFactory2(){
        ConcurrentKafkaListenerContainerFactory<String,Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory2());
        return factory;
    }

    public ConsumerFactory<String,Object> consumerFactory2(){
        return new DefaultKafkaConsumerFactory<>(consumerConfig2());
    }

    public Map<String,Object> consumerConfig2(){
        Map<String,Object> props =  new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }


    @Bean
    public KafkaTemplate<String,Object> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

    public ProducerFactory<String,Object> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    public Map<String,Object> producerConfig(){
        Map<String,Object> props =  new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return props;
    }

    @Bean(name = "stringSerializer")
    public KafkaTemplate<String,Object> kafkaTemplate2(){
        return new KafkaTemplate<>(producerFactory2());
    }

    public ProducerFactory<String,Object> producerFactory2(){
        return new DefaultKafkaProducerFactory<>(producerConfig2());
    }

    public Map<String,Object> producerConfig2(){
        Map<String,Object> props =  new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

}
