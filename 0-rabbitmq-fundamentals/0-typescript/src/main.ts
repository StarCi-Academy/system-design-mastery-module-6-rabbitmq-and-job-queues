/**
 * Entry point: dispatch to the producer API or a consumer based on the APP env var.
 * Using one image for all services keeps the Docker build simple — compose sets
 * APP=producer for the HTTP API and APP=consumer for every consumer container.
 */
// Read APP before requiring the module so TypeScript compiles both branches.
const app = process.env.APP ?? "producer"

if (app === "producer") {
  require("./producer")
} else {
  require("./consumer")
}
