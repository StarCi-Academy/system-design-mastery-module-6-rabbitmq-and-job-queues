# syntax=docker/dockerfile:1

# ---- Build stage: compile a static binary ----
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /rate-api .

# ---- Runtime stage: minimal image ----
FROM alpine:3.20 AS runtime
WORKDIR /app
COPY --from=build /rate-api /app/rate-api
CMD ["/app/rate-api"]
