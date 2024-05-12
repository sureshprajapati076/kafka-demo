package com.kafka.producer.config;

import com.kafka.producer.util.TopicConstants;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {


    // Below NewTopic Beans are for easy demo purposes only, in real time, its created by cli only.
    @Bean
    public NewTopic carTopic(){
        return TopicBuilder
                .name(TopicConstants.CAR_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic personTopic(){
        return TopicBuilder
                .name(TopicConstants.PERSON_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic textOnlyTopic(){
        return TopicBuilder
                .name(TopicConstants.TEXT_ONLY_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

/*

    Create topic from CLI:

    C:\kafka_2.12-3.7.0> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

    C:\kafka_2.12-3.7.0> .\bin\windows\kafka-server-start.bat .\config\server.properties

    C:\kafka_2.12-3.7.0\bin\windows> kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic mytopic

*/

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
