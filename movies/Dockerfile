FROM eclipse-temurin:23-jdk-noble AS builder

ARG COMPILE_DIR=/compiledir
WORKDIR /src

COPY mvnw .
COPY pom.xml .
COPY src src
COPY .mvn .mvn
COPY ../data data

RUN chmod a+x ./mvnw && ./mvnw package -Dmaven.test.skip=true

FROM eclipse-temurin:23-jre-noble

WORKDIR /app

COPY --from=builder /src/target/movies-0.0.1-SNAPSHOT.jar b5_paf.jar

ENV PORT=8080
EXPOSE ${PORT}

ENV SPRING_DATA_MONGODB_URI="" SPRING_DATA_MONGODB_DATABASE=""
ENV SPRING_DATASOURCE_URL="" SPRING_DATASOURCE_USERNAME="" SPRING_DATASOURCE_PASSWORD=""

ENV DS_NAME="" DS_BATCH=""

ENTRYPOINT SERVER_PORT=${PORT} java -jar b5_paf.jar data/movies_post_2010.zip