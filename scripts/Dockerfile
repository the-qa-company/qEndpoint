FROM maven:3.9.9-amazoncorretto-23-debian-bookworm AS build-backend

WORKDIR /app

RUN apt-get update
RUN apt-get install wget -y

COPY qendpoint.jar .

ARG MEM_SIZE=6G
ENV MEM_SIZE ${MEM_SIZE}

EXPOSE 1234
ENTRYPOINT java -jar -Xmx${MEM_SIZE}  qendpoint.jar
