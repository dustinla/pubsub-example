package dev.dustinla.pubsub.config;

import com.google.cloud.spring.pubsub.support.converter.ConvertedBasicAcknowledgeablePubsubMessage;

import java.util.function.Consumer;

public interface PubSubConsumer<T> {

    String subscription();

    Class<T> payloadType();

    Consumer<ConvertedBasicAcknowledgeablePubsubMessage<T>> messageConsumer();
}