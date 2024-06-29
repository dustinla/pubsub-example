package dev.dustinla.pubsub.config;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import dev.dustinla.pubsub.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;

@Configuration
@Slf4j
public class PubSubIntegrationConfig {

    @Value("${pubsub.subscriptionOne}")
    private String subscription;


    @Bean
    public MessageChannel myInputChannel() {
        return new DirectChannel();
    }
    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(
            @Qualifier("myInputChannel") MessageChannel inputChannel,
            PubSubTemplate pubSubTemplate) {
        log.info("Subscribing to {}", subscription);
        PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate, subscription);
        adapter.setOutputChannel(inputChannel);
        adapter.setPayloadType(Message.class);
        return adapter;
    }
}
