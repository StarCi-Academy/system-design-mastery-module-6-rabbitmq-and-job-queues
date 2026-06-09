# syntax=docker/dockerfile:1

# ---- Build stage: compile a static Go binary ----
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY main.go ./
# Resolve the dependency graph and write go.sum, then build a static binary.
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /app/server .

# ---- Runtime stage: minimal image with just the binary ----
FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=build /app/server ./server
CMD ["./server"]
