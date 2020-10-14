FROM maven:3.6.0-jdk-11-slim 
COPY ./ ./
COPY pom.xml ./
COPY ci_settings.xml ./
RUN mvn deploy -s ci_settings.xml