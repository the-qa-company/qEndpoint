FROM maven:3.6-jdk-11

COPY src /home/app/src
COPY pom.xml /home/app
COPY ci_settings.xml ./
RUN mvn deploy -s ci_settings.xml