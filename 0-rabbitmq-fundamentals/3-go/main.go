// Package main is the single binary for all roles (producer + 4 consumers).
// APP=producer starts the HTTP API; APP=consumer + ROLE selects the consumer behaviour.
package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Shared AMQP topology: the producer always publishes to an EXCHANGE, and the
// exchange type (direct/fanout/topic) decides routing. Constants kept in one
// place so producer and consumers always agree on names.
const (
	// exchangeDirect routes by an exact routing-key match — 1:1 routing.
	exchangeDirect = "orders.direct"
	// exchangeFanout ignores the routing key and broadcasts to every bound queue.
	exchangeFanout = "events.fanout"
	// exchangeTopic routes by wildcard pattern (* = one token, # = zero or more).
	exchangeTopic = "logs.topic"

	// queueDirect is bound to exchangeDirect with directRoutingKey.
	queueDirect = "orders.created.q"
	// queueFanoutA is fanout consumer instance A — each instance needs its own queue.
	queueFanoutA = "events.fanout.q.a"
	// queueFanoutB is fanout consumer instance B.
	queueFanoutB = "events.fanout.q.b"
	// queueTopic is bound to exchangeTopic with topicPattern.
	queueTopic = "logs.topic.q"

	// directRoutingKey is the exact binding key; any other key is dropped.
	directRoutingKey = "orders.created"
	// topicPattern: # matches zero or more dot-separated tokens.
	topicPattern = "logs.#"
)

// publishResult is the JSON envelope returned by every /publish/* endpoint.
// It carries enough context for the caller to correlate the HTTP response
// with the broker message without polling the queue.
type publishResult struct {
	Exchange    string      `json:"exchange"`    // AMQP exchange the message was sent to.
	RoutingKey  string      `json:"routingKey"`  // Routing key used; empty for fanout.
	Payload     interface{} `json:"payload"`     // Original request body forwarded as-is.
	PublishedAt string      `json:"publishedAt"` // ISO-8601 timestamp of the publish call.
}

// env reads key from the environment; returns def when the key is unset or empty.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// connectWithRetry retries because the app usually starts before the broker.
// It returns the open connection on success or calls log.Fatal after 10 attempts.
func connectWithRetry(url string) *amqp.Connection {
	for attempt := 1; attempt <= 10; attempt++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn
		}
		log.Printf("broker not ready (attempt %d/10), retrying...", attempt)
		time.Sleep(2 * time.Second)
	}
	log.Fatal("Failed to connect to RabbitMQ after 10 attempts")
	return nil
}

// runProducer connects to the broker, declares all three exchanges, then starts
// an HTTP server with /health and three /publish/* endpoints.
func runProducer(url string) {
	conn := connectWithRetry(url)
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	// assertExchange equivalent: durable so the topology survives a broker restart.
	for _, ex := range []struct{ name, kind string }{
		{exchangeDirect, "direct"},
		{exchangeFanout, "fanout"},
		{exchangeTopic, "topic"},
	} {
		if err := ch.ExchangeDeclare(ex.name, ex.kind, true, false, false, false, nil); err != nil {
			log.Fatal(err)
		}
	}
	log.Println("[producer-api] connected to broker, three exchanges declared")

	publish := func(exchange, routingKey string, payload interface{}) publishResult {
		body, _ := json.Marshal(payload)
		// The producer talks to an EXCHANGE, never a queue.
		_ = ch.Publish(exchange, routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		})
		return publishResult{exchange, routingKey, payload, time.Now().UTC().Format(time.RFC3339Nano)}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/publish/direct/", func(w http.ResponseWriter, r *http.Request) {
		rk := strings.TrimPrefix(r.URL.Path, "/publish/direct/")
		writeJSON(w, publish(exchangeDirect, rk, readBody(r)))
	})
	mux.HandleFunc("/publish/fanout", func(w http.ResponseWriter, r *http.Request) {
		// Fanout ignores the routing key, so publish with an empty key "".
		writeJSON(w, publish(exchangeFanout, "", readBody(r)))
	})
	mux.HandleFunc("/publish/topic/", func(w http.ResponseWriter, r *http.Request) {
		rk := strings.TrimPrefix(r.URL.Path, "/publish/topic/")
		writeJSON(w, publish(exchangeTopic, rk, readBody(r)))
	})

	port := env("PORT", "3000")
	log.Printf("[producer-api] listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// readBody reads the entire request body and unmarshals it into an interface{}.
func readBody(r *http.Request) interface{} {
	var payload interface{}
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(r.Body)
	_ = json.Unmarshal(buf.Bytes(), &payload)
	return payload
}

// writeJSON serialises v to JSON and writes it to w with Content-Type: application/json.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(v)
}

// consume starts a blocking message loop on queue.
// label is the log prefix (e.g. "[DIRECT]", "[FANOUT][a]").
// withRoutingKey appends [rk=...] to each log line so learners can verify routing.
func consume(ch *amqp.Channel, queue, label string, withRoutingKey bool) {
	// Manual ack: an un-acked message is redelivered if the consumer crashes.
	// prefetch=1 → fair dispatch: broker hands one message at a time to each free consumer.
	_ = ch.Qos(1, 0, false)
	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	for d := range msgs {
		if withRoutingKey {
			log.Printf("%s[rk=%s] %s", label, d.RoutingKey, string(d.Body))
		} else {
			log.Printf("%s %s", label, string(d.Body))
		}
		_ = d.Ack(false)
	}
}

// runConsumer connects to the broker, declares the queue/binding for the given
// role, then enters the consume loop. Role is one of: direct | fanout-a | fanout-b | topic.
func runConsumer(url, role string) {
	conn := connectWithRetry(url)
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	switch role {
	case "direct":
		_ = ch.ExchangeDeclare(exchangeDirect, "direct", true, false, false, false, nil)
		_, _ = ch.QueueDeclare(queueDirect, true, false, false, false, nil)
		// Direct routing: deliver only when the routing key EXACTLY equals this binding key.
		_ = ch.QueueBind(queueDirect, directRoutingKey, exchangeDirect, false, nil)
		log.Println("[consumer-direct] bound and waiting for messages")
		consume(ch, queueDirect, "[DIRECT]", true)
	case "fanout-a", "fanout-b":
		queue := queueFanoutA
		inst := "a"
		if role == "fanout-b" {
			queue, inst = queueFanoutB, "b"
		}
		_ = ch.ExchangeDeclare(exchangeFanout, "fanout", true, false, false, false, nil)
		_, _ = ch.QueueDeclare(queue, true, false, false, false, nil)
		// Fanout ignores the routing key, so bind with an empty key "".
		_ = ch.QueueBind(queue, "", exchangeFanout, false, nil)
		log.Printf("[consumer-fanout-%s] bound and waiting for messages", inst)
		consume(ch, queue, "[FANOUT]["+inst+"]", false)
	case "topic":
		_ = ch.ExchangeDeclare(exchangeTopic, "topic", true, false, false, false, nil)
		_, _ = ch.QueueDeclare(queueTopic, true, false, false, false, nil)
		// Topic pattern: "#" matches zero or more tokens. "logs.#" matches any key under "logs.".
		_ = ch.QueueBind(queueTopic, topicPattern, exchangeTopic, false, nil)
		log.Println("[consumer-topic] bound and waiting for messages")
		consume(ch, queueTopic, "[TOPIC]", true)
	default:
		log.Fatalf("Unknown ROLE: %s", role)
	}
}

// main reads APP and ROLE from the environment and dispatches to the appropriate role.
// Using one binary for all roles keeps the Docker image small and the build simple.
func main() {
	url := env("RABBITMQ_URL", "amqp://starci:Cuong123_A@rabbitmq:5672")
	// APP=producer → HTTP API; APP=consumer → message consumer selected by ROLE.
	if env("APP", "producer") == "producer" {
		runProducer(url)
	} else {
		runConsumer(url, env("ROLE", "direct"))
	}
}
