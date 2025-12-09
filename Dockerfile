FROM maven:3.9-eclipse-temurin-21

WORKDIR /app

# Copy project files
COPY pom.xml .
COPY src ./src
COPY control ./control

# Download dependencies and compile
RUN mvn dependency:go-offline -B && mvn compile

# Default main class (overridden in docker-compose)
ENV MAIN_CLASS=bigdatastage3.SearchAPI

EXPOSE 7003

CMD ["sh", "-c", "mvn exec:java -Dexec.mainClass=$MAIN_CLASS -q"]