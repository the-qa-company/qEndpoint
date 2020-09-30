FROM maven:3.6-jdk-11
ENV JOB_TOKEN=$CI_JOB_TOKEN
COPY src ./src
COPY pom.xml ./
COPY ci_settings.xml ./
RUN mvn deploy -s ci_settings.xml