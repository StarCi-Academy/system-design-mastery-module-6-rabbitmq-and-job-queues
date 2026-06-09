# syntax=docker/dockerfile:1

# ---- Build stage: resolve deps and package the fat jar ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml ./
RUN mvn -q -e dependency:go-offline
COPY src ./src
RUN mvn -q -e clean package -DskipTests

# ---- Runtime stage: JRE only + the packaged jar ----
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
COPY --from=build /app/target/app.jar /app/app.jar
CMD ["java", "-jar", "/app/app.jar"]
