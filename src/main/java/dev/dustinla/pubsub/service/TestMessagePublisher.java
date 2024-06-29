package dev.dustinla.pubsub.service;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import dev.dustinla.pubsub.model.Message;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TestMessagePublisher implements Publisher<Message>{


    @Value("${pubsub.topic}")
    private String topic;

    private final PubSubTemplate pubSubTemplate;

    @Override
    public void publish(Message message) {
        pubSubTemplate.publish(topic, message);

    }

}
