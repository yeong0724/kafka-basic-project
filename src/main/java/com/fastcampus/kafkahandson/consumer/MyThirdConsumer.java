package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@Component
public class MyThirdConsumer {
    /**
     * 하나의 Topic 을 가지고 2개 이상의 Consumer Group 이 Consume 하기 위해 groupId 를 반드시 서로 다르게 설정해준다.
     */
    @KafkaListener(
            topics = {MY_JSON_TOPIC},
            groupId = "batch-test-consumer-group",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void accept(List<ConsumerRecord<String, String>> messages) {
        ObjectMapper objectMapper = new ObjectMapper();

        messages.forEach(message -> {
            MyMessage myMessage;
                    try {
                        myMessage = objectMapper.readValue(message.value(), MyMessage.class);

                        System.out.println("ㄴ [Third Consumer] Value - " +
                                myMessage + " / Offset - " +
                                message.offset() + " / Partition - " +
                                message.partition()
                        );
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }
}
