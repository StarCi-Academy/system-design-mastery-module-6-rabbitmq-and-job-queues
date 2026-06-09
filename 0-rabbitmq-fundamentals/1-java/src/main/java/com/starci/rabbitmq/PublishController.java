package com.starci.rabbitmq;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP controller that exposes three publish endpoints — one per exchange type.
 * Only active when this process runs as the producer (app.role=producer).
 * The producer always sends to an EXCHANGE, never directly to a queue.
 */
@RestController
@ConditionalOnProperty(name = "app.role", havingValue = "producer")
public class PublishController {

    /** Spring AMQP template used for all convertAndSend calls. */
    private final RabbitTemplate rabbitTemplate;
    /** Jackson mapper converts the request body to raw JSON bytes for the wire. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructor injection — Spring wires the auto-configured RabbitTemplate.
     *
     * @param rabbitTemplate the Spring AMQP template
     */
    public PublishController(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * Liveness check — confirms the producer-api container is up and the
     * Spring context has finished wiring (including exchange declarations).
     *
     * @return 200 OK with {@code {"status":"ok"}}
     */
    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }

    /**
     * Publishes to the direct exchange using the given routing key.
     * Only queues bound with this exact key will receive the message.
     *
     * @param routingKey the binding key to match (e.g. "orders.created")
     * @param payload    arbitrary JSON body forwarded to the exchange
     * @return publish envelope with exchange / routingKey / payload / publishedAt
     */
    @PostMapping("/publish/direct/{routingKey}")
    public Map<String, Object> publishDirect(@PathVariable String routingKey, @RequestBody Object payload) {
        return publish(Topology.EXCHANGE_DIRECT, routingKey, payload);
    }

    /**
     * Publishes to the fanout exchange; routing key is irrelevant for fanout.
     * Every queue bound to this exchange receives a copy of the message.
     *
     * @param payload arbitrary JSON body
     * @return publish envelope
     */
    @PostMapping("/publish/fanout")
    public Map<String, Object> publishFanout(@RequestBody Object payload) {
        // Fanout ignores the routing key, so publish with an empty key "".
        return publish(Topology.EXCHANGE_FANOUT, "", payload);
    }

    /**
     * Publishes to the topic exchange using the given routing key.
     * Consumer queues are bound with patterns like "logs.#" that may match.
     *
     * @param routingKey the routing key to match against consumer patterns
     * @param payload    arbitrary JSON body
     * @return publish envelope
     */
    @PostMapping("/publish/topic/{routingKey}")
    public Map<String, Object> publishTopic(@PathVariable String routingKey, @RequestBody Object payload) {
        return publish(Topology.EXCHANGE_TOPIC, routingKey, payload);
    }

    /**
     * Core publish helper: serialises the payload to JSON bytes and sends it
     * to the given exchange via RabbitTemplate. Returns an envelope that the
     * HTTP caller can use for correlation.
     *
     * @param exchange   target exchange name
     * @param routingKey routing hint; fanout ignores it
     * @param payload    request body to forward
     * @return map with exchange, routingKey, payload, publishedAt
     */
    private Map<String, Object> publish(String exchange, String routingKey, Object payload) {
        // The producer talks to an EXCHANGE, never a queue. Send the payload as
        // raw JSON bytes so every language track shares the same wire format.
        byte[] body;
        try {
            body = objectMapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            // Serialisation failure is a programming error, not a recoverable runtime condition.
            throw new RuntimeException(e);
        }
        rabbitTemplate.convertAndSend(exchange, routingKey, body);
        // Build the response envelope in insertion order for readable JSON output.
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("exchange", exchange);
        result.put("routingKey", routingKey);
        result.put("payload", payload);
        result.put("publishedAt", Instant.now().toString());
        return result;
    }
}
