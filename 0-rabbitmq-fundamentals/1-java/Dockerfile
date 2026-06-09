# syntax=docker/dockerfile:1

# ---- Build stage: resolve dependencies and package the Spring Boot jar ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml ./
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B clean package -DskipTests

# ---- Runtime stage: JRE + the fat jar ----
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build /app/target/rabbitmq-fundamentals-1.0.0.jar ./app.jar
CMD ["java", "-jar", "app.jar"]
