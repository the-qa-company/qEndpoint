#
# Build stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY src /home/app/src
COPY pom.xml /home/app
COPY ci_settings.xml /home/app
RUN mvn -f /home/app/pom.xml clean package -s /home/app/ci_settings.xml --quiet

#
# Package stage
#
FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/hdtSparqlEndpoint-*-SNAPSHOT.jar /usr/local/lib/hdtSparqlEndpoint-*-SNAPSHOT.jar
EXPOSE 1234
ENTRYPOINT ["java","-jar","/usr/local/lib/hdtSparqlEndpoint-*-SNAPSHOT.jar"]
