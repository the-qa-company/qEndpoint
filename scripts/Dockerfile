FROM maven:3.6.0-jdk-11-slim AS build-backend

RUN apt-get update
RUN apt-get install wget -y

RUN wget https://github.com/the-qa-company/qEndpoint/releases/latest/download/endpoint.jar

ARG MEM_SIZE=6G
ENV MEM_SIZE ${MEM_SIZE}

EXPOSE 1234
ENTRYPOINT java -jar -Xmx${MEM_SIZE}  endpoint.jar