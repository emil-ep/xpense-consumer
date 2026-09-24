FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /app

ARG GITHUB_TOKEN
ARG GITHUB_ACTOR

RUN mkdir -p ~/.m2 \
 && echo "<settings><servers><server><id>github</id><username>${GITHUB_ACTOR}</username><password>${GITHUB_TOKEN}</password></server></servers></settings>" > ~/.m2/settings.xml

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean install -DskipTests

FROM eclipse-temurin:21-jdk
WORKDIR /app
COPY --from=builder /app/target/*.jar /app.jar
EXPOSE 8089
ENTRYPOINT ["java", "-jar", "/app.jar"]
