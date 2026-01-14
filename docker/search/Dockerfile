# Dockerfile
FROM eclipse-temurin:17-jre

WORKDIR /app

COPY *.jar app.jar

ENV JAVA_OPTS=""
ENV MAIN_CLASS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -cp app.jar $MAIN_CLASS"]
