package com.starci.rabbitmq;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the three AMQP exchanges as Spring beans so they are available
 * in every process — the producer and the consumers agree on the exchange
 * types at startup. The exchange type (direct/fanout/topic) decides routing.
 * Spring AMQP / RabbitAdmin detects these beans and ensures the exchanges
 * exist in the broker, making the declare idempotent.
 */
@Configuration
public class ExchangeConfig {

    /**
     * Direct exchange: delivers a message only to queues whose binding key
     * exactly matches the message's routing key — 1:1 routing.
     *
     * @return a durable, non-auto-delete DirectExchange bean
     */
    @Bean
    public DirectExchange ordersDirect() {
        // durable=true keeps the exchange across a broker restart; autoDelete=false prevents removal when unused.
        return new DirectExchange(Topology.EXCHANGE_DIRECT, true, false);
    }

    /**
     * Fanout exchange: ignores the routing key entirely and broadcasts every
     * message to all bound queues — one-to-many routing.
     *
     * @return a durable, non-auto-delete FanoutExchange bean
     */
    @Bean
    public FanoutExchange eventsFanout() {
        // Fanout never inspects the routing key, so binding key is irrelevant.
        return new FanoutExchange(Topology.EXCHANGE_FANOUT, true, false);
    }

    /**
     * Topic exchange: routes by wildcard pattern (* = one token, # = zero or more
     * dot-separated tokens) — the most flexible exchange type.
     *
     * @return a durable, non-auto-delete TopicExchange bean
     */
    @Bean
    public TopicExchange logsTopic() {
        // Pattern matching cost is higher than direct but enables multi-dimension routing.
        return new TopicExchange(Topology.EXCHANGE_TOPIC, true, false);
    }
}
