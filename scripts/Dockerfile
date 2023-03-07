FROM maven:3.8.5-openjdk-17-slim AS build-backend

WORKDIR /app

RUN apt-get update
RUN apt-get install wget -y

COPY qendpoint.jar .

ARG MEM_SIZE=6G
ENV MEM_SIZE ${MEM_SIZE}

EXPOSE 1234
ENTRYPOINT java -jar -Xmx${MEM_SIZE}  qendpoint.jar
