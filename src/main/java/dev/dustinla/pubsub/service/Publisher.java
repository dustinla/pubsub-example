package dev.dustinla.pubsub.service;

public interface Publisher<T> {

    void publish(T message);
}
