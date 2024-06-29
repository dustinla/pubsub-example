package dev.dustinla.pubsub.consumer;

import com.google.cloud.spring.pubsub.support.converter.ConvertedBasicAcknowledgeablePubsubMessage;
import com.google.pubsub.v1.ProjectSubscriptionName;
import dev.dustinla.pubsub.config.PubSubConsumer;
import dev.dustinla.pubsub.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Component
@Slf4j
public class ExampleConsumer implements PubSubConsumer<Message> {

    private final String subscriptionName;

    public ExampleConsumer(@Value("${pubsub.subscriptionTwo}") String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    @Override
    public String subscription() {
        return subscriptionName;
    }

    @Override
    public Class<Message> payloadType() {
        return Message.class;
    }

    @Override
    public Consumer<ConvertedBasicAcknowledgeablePubsubMessage<Message>> messageConsumer() {
        return this::consume;
    }

    private void consume(ConvertedBasicAcknowledgeablePubsubMessage<Message> message) {
        Message received = message.getPayload();
        ProjectSubscriptionName projectSubscriptionName = message.getProjectSubscriptionName();

        log.info("{} - Received entity: [data= {}, info= {}]",
                projectSubscriptionName.getSubscription(),
                received.data(),
                received.info());

        message.ack();
    }
}