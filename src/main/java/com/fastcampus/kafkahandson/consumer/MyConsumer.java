package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;


@Component
public class MyConsumer {

    @KafkaListener(
            topics = {MY_JSON_TOPIC},
            groupId = "test-consumer-group"
    )
    public void accept(ConsumerRecord<String, MyMessage> message) {
        /**
         * Consumer 의 message 는 Key-Value 형태의 쌍으로 이루어져 있다.
         */
        System.out.println("[Main Consumer] Message arrived! - " + message.value());
    }
}