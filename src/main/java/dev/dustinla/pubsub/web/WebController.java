package dev.dustinla.pubsub.web;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.support.AcknowledgeablePubsubMessage;
import com.google.pubsub.v1.PubsubMessage;
import dev.dustinla.pubsub.model.Message;
import dev.dustinla.pubsub.service.TestMessagePublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@RestController
@Slf4j
@RequiredArgsConstructor
public class WebController {

    private final TestMessagePublisher testMessagePublisher;

    private final PubSubTemplate pubSubTemplate;

    private final PubSubAdmin pubSubAdmin;

    private final ArrayList<Subscriber> allSubscribers;


    @PostMapping("/createTopic")
    public RedirectView createTopic(@RequestParam("topicName") String topicName) {
        this.pubSubAdmin.createTopic(topicName);

        return buildStatusView("Topic creation successful.");
    }

    @PostMapping("/createSubscription")
    public RedirectView createSubscription(
            @RequestParam("topicName") String topicName,
            @RequestParam("subscriptionName") String subscriptionName) {
        this.pubSubAdmin.createSubscription(subscriptionName, topicName);

        return buildStatusView("Subscription creation successful.");
    }

    @GetMapping("/postMessage")
    public ResponseEntity<String> publish(
            @RequestParam("message") String message,
            @RequestParam("info") String info,
            @RequestParam(value = "count", defaultValue = "1") int count) {

        for (int co = 0; co < count; co++) {
            testMessagePublisher.publish(new Message(message, info));
            log.info("published: count: {}, message {}, info {}", count, message, info);
        }

        return ResponseEntity.ok().body(String.format("published: count: %d, message %s, info %s", count, message, info));
    }

    @GetMapping("/pull")
    public RedirectView pull(@RequestParam("subscription1") String subscriptionName) {

        Collection<AcknowledgeablePubsubMessage> messages =
                this.pubSubTemplate.pull(subscriptionName, 10, true);

        return handleMessage(messages);
    }

    @GetMapping("/multipull")
    public RedirectView multipull(
            @RequestParam("subscription1") String subscriptionName1,
            @RequestParam("subscription2") String subscriptionName2) {

        Set<AcknowledgeablePubsubMessage> mixedSubscriptionMessages = new HashSet<>();
        mixedSubscriptionMessages.addAll(this.pubSubTemplate.pull(subscriptionName1, 1000, true));
        mixedSubscriptionMessages.addAll(this.pubSubTemplate.pull(subscriptionName2, 1000, true));

        return handleMessage(mixedSubscriptionMessages);
    }

    @GetMapping("/subscribe")
    public RedirectView subscribe(@RequestParam("subscription") String subscriptionName) {
        Subscriber subscriber =
                this.pubSubTemplate.subscribe(
                        subscriptionName,
                        message -> {
                            log.info(
                                    "Message received from "
                                            + subscriptionName
                                            + " subscription: "
                                            + message.getPubsubMessage().getData().toStringUtf8());
                            message.ack();
                        });

        this.allSubscribers.add(subscriber);
        return buildStatusView("Subscribed.");
    }

    @PostMapping("/deleteTopic")
    public RedirectView deleteTopic(@RequestParam("topic") String topicName) {
        this.pubSubAdmin.deleteTopic(topicName);

        return buildStatusView("Topic deleted successfully.");
    }

    @PostMapping("/deleteSubscription")
    public RedirectView deleteSubscription(@RequestParam("subscription") String subscriptionName) {
        this.pubSubAdmin.deleteSubscription(subscriptionName);

        return buildStatusView("Subscription deleted successfully.");
    }

    private RedirectView buildStatusView(String statusMessage) {
        RedirectView view = new RedirectView("/");
        view.addStaticAttribute("statusMessage", statusMessage);
        return view;
    }

    private RedirectView handleMessage(Collection<AcknowledgeablePubsubMessage> messages) {
        if (messages.isEmpty()) {
            return buildStatusView("No messages available for retrieval.");
        }
        messages.forEach(acknowledgeablePubsubMessage -> {
            String ackId = acknowledgeablePubsubMessage.getAckId();
            PubsubMessage pubsubMessage = acknowledgeablePubsubMessage.getPubsubMessage();
            String stringUtf8 = pubsubMessage.getData().toStringUtf8();
            log.info("Message received from ACK: {}: Message: {}", ackId, stringUtf8);
        });

        RedirectView returnView;
        try {
            CompletableFuture<Void> ackFuture = this.pubSubTemplate.ack(messages);
            ackFuture.get();
            returnView =
                    buildStatusView(
                            String.format("Pulled and acked %s message(s)", messages.size()));
        } catch (Exception ex) {
            log.warn("Acking failed.", ex);
            returnView = buildStatusView("Acking failed");
        }

        return returnView;
    }
}