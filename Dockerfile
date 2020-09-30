FROM maven:3.6-jdk-11

COPY src ./src
COPY pom.xml ./
COPY ci_settings.xml ./
RUN mvn deploy -s ci_settings.xml