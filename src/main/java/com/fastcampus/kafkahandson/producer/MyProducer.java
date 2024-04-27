package com.fastcampus.kafkahandson.producer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import static com.fastcampus.kafkahandson.model.Topic.MY_JSON_TOPIC;

@RequiredArgsConstructor
@Component
public class MyProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    ObjectMapper objectMapper = new ObjectMapper();

    public void sendMessage(MyMessage myMessage) throws JsonProcessingException {
        /**
         * message key 지정 -> age
         * Producing 할때 String Serializer 를 쓰지만 String 으로 직접 데이터를 넣어줘서 문제없이 발송할수 있도록 수정
         */
        kafkaTemplate.send(
                MY_JSON_TOPIC,
                String.valueOf(myMessage.getAge()),
                objectMapper.writeValueAsString(myMessage)
        );
    }

}