package com.starci.rabbitmq;

import java.nio.charset.StandardCharsets;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Declares all four queues, their bindings to the three exchanges, and the
 * {@link RabbitListener} handler methods that process each incoming message.
 * Only active when this process runs as a consumer (app.role=consumer).
 * Spring AMQP / RabbitAdmin creates the queues and bindings on startup;
 * acks are automatic after the handler method returns normally (default mode).
 */
@Component
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
public class Consumers {

    /**
     * Durable queue for the direct consumer.
     * Survives a broker restart — no messages are lost if the consumer is down.
     *
     * @return a durable, non-exclusive, non-auto-delete Queue bean
     */
    @Bean
    public Queue ordersQueue() {
        return new Queue(Topology.QUEUE_DIRECT, true);
    }

    /**
     * Durable queue for fanout consumer instance A.
     *
     * @return a durable Queue bean for instance A
     */
    @Bean
    public Queue fanoutQueueA() {
        return new Queue(Topology.QUEUE_FANOUT_A, true);
    }

    /**
     * Durable queue for fanout consumer instance B.
     * A separate queue is required so both A and B receive every broadcast.
     *
     * @return a durable Queue bean for instance B
     */
    @Bean
    public Queue fanoutQueueB() {
        return new Queue(Topology.QUEUE_FANOUT_B, true);
    }

    /**
     * Durable queue for the topic consumer.
     *
     * @return a durable Queue bean for the topic pattern subscriber
     */
    @Bean
    public Queue logsQueue() {
        return new Queue(Topology.QUEUE_TOPIC, true);
    }

    /**
     * Binding: direct exchange → orders queue, routing key = DIRECT_ROUTING_KEY.
     * Direct routing: deliver only when the routing key EXACTLY equals this binding key.
     *
     * @param ordersQueue   the durable direct queue
     * @param ordersDirect  the direct exchange bean
     * @return the Binding between queue and exchange
     */
    @Bean
    public Binding ordersBinding(Queue ordersQueue, DirectExchange ordersDirect) {
        return BindingBuilder.bind(ordersQueue).to(ordersDirect).with(Topology.DIRECT_ROUTING_KEY);
    }

    /**
     * Binding: fanout exchange → queue A.
     * Fanout ignores the routing key and broadcasts to every bound queue.
     *
     * @param fanoutQueueA  instance A's queue
     * @param eventsFanout  the fanout exchange bean
     * @return the Binding between queue A and the fanout exchange
     */
    @Bean
    public Binding fanoutBindingA(Queue fanoutQueueA, FanoutExchange eventsFanout) {
        // No routing key needed — fanout delivers to all bound queues unconditionally.
        return BindingBuilder.bind(fanoutQueueA).to(eventsFanout);
    }

    /**
     * Binding: fanout exchange → queue B.
     *
     * @param fanoutQueueB  instance B's queue
     * @param eventsFanout  the fanout exchange bean
     * @return the Binding between queue B and the fanout exchange
     */
    @Bean
    public Binding fanoutBindingB(Queue fanoutQueueB, FanoutExchange eventsFanout) {
        return BindingBuilder.bind(fanoutQueueB).to(eventsFanout);
    }

    /**
     * Binding: topic exchange → logs queue, pattern = TOPIC_PATTERN ("logs.#").
     * Topic pattern "logs.#" matches any routing key under "logs.".
     *
     * @param logsQueue  the topic queue
     * @param logsTopic  the topic exchange bean
     * @return the Binding between the logs queue and the topic exchange
     */
    @Bean
    public Binding logsBinding(Queue logsQueue, TopicExchange logsTopic) {
        return BindingBuilder.bind(logsQueue).to(logsTopic).with(Topology.TOPIC_PATTERN);
    }

    /**
     * Receives messages from the direct queue and logs each one.
     * Spring AMQP auto-acks after this method returns; a crash triggers redelivery.
     *
     * @param body       raw JSON bytes of the message
     * @param routingKey the original routing key, injected from the AMQP header
     */
    @RabbitListener(queues = Topology.QUEUE_DIRECT)
    public void onDirect(byte[] body, @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) String routingKey) {
        // Print the routing key so learners can observe the exact-match rule in the console log.
        System.out.println("[DIRECT][rk=" + routingKey + "] " + new String(body, StandardCharsets.UTF_8));
    }

    /**
     * Receives messages from fanout queue A (instance A of the broadcast).
     *
     * @param body raw JSON bytes of the message
     */
    @RabbitListener(queues = Topology.QUEUE_FANOUT_A)
    public void onFanoutA(byte[] body) {
        System.out.println("[FANOUT][a] " + new String(body, StandardCharsets.UTF_8));
    }

    /**
     * Receives messages from fanout queue B (instance B of the broadcast).
     * Both onFanoutA and onFanoutB log the same message — confirming the broadcast.
     *
     * @param body raw JSON bytes of the message
     */
    @RabbitListener(queues = Topology.QUEUE_FANOUT_B)
    public void onFanoutB(byte[] body) {
        System.out.println("[FANOUT][b] " + new String(body, StandardCharsets.UTF_8));
    }

    /**
     * Receives messages from the topic queue that match the "logs.#" pattern.
     *
     * @param body       raw JSON bytes of the message
     * @param routingKey the original routing key, injected from the AMQP header
     */
    @RabbitListener(queues = Topology.QUEUE_TOPIC)
    public void onTopic(byte[] body, @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) String routingKey) {
        // Logging the routing key lets learners verify the wildcard pattern matching rule.
        System.out.println("[TOPIC][rk=" + routingKey + "] " + new String(body, StandardCharsets.UTF_8));
    }
}
