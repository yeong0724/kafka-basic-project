package com.fastcampus.kafkahandson.producer;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_SECOND_TOPIC;

@Component
public class MySecondProducer {
    @Qualifier("secondKafkaTemplate")
    private final KafkaTemplate<String, String> secondKafkaTemplate;

    public MySecondProducer(KafkaTemplate<String, String> secondKafkaTemplate) {
        this.secondKafkaTemplate = secondKafkaTemplate;
    }

    public void sendMessageWithKey(String key, String message) {
        secondKafkaTemplate.send(MY_SECOND_TOPIC, key, message);
    }
}
