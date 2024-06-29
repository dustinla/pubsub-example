package dev.dustinla.pubsub.config;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import dev.dustinla.pubsub.consumer.ExampleConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class PubSubExampleConfig {

    private final PubSubTemplate pubSubTemplate;
    private final ExampleConsumer exampleConsumer;

    @EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        log.info("Subscribing to {}", exampleConsumer.subscription());

        pubSubTemplate.subscribeAndConvert(
                exampleConsumer.subscription(),
                exampleConsumer.messageConsumer(),
                exampleConsumer.payloadType()
        );
    }
}
