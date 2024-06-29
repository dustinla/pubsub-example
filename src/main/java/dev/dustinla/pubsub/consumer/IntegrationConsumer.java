package dev.dustinla.pubsub.consumer;

import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import dev.dustinla.pubsub.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class IntegrationConsumer {

    @ServiceActivator(inputChannel = "myInputChannel")
    public void messageReceiver(Message payload,
                                @Header(GcpPubSubHeaders.ORIGINAL_MESSAGE) BasicAcknowledgeablePubsubMessage message) {
        log.info("{} - Message received from Google Cloud Pub/Sub: {}",
                message.getProjectSubscriptionName().getSubscription(),
                payload);

        // Acknowledge the message upon successful processing
        message.ack();
    }

}