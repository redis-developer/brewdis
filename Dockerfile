FROM openjdk:13-jdk-slim AS build
WORKDIR /app
COPY . /app
RUN ./gradlew build --no-daemon

#### Stage 2: A minimal docker image with command to run the app 
FROM openjdk:13-jdk-slim

EXPOSE 8080

RUN mkdir /app

COPY --from=build /app/app/brewdis-api/build/libs/*.jar /app/brewdis.jar

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app/brewdis.jar"]
