# ---- Build stage ----
FROM maven:3.9.6-eclipse-temurin-21 AS build

WORKDIR /app
COPY . .

# Install unzip
RUN apt-get update && apt-get install -y unzip && rm -rf /var/lib/apt/lists/*

# Build project
RUN mvn clean package -DskipTests

# Unzip the distribution created by maven-assembly-plugin
RUN unzip target/DiagnosticEngineApp-*-distribution.zip -d target/

# ---- Runtime stage ----
FROM eclipse-temurin:21-jre

WORKDIR /DiagnosticEngineApp

# Copy final JAR and lib folder from distribution to runtime image
COPY --from=build /app/target/DiagnosticEngineApp-1.0-SNAPSHOT/DiagnosticEngineApp-1.0-SNAPSHOT.jar /DiagnosticEngineApp/
COPY --from=build /app/target/DiagnosticEngineApp-1.0-SNAPSHOT/lib /DiagnosticEngineApp/lib

# Run the application
ENTRYPOINT ["java", "-jar", "/DiagnosticEngineApp/DiagnosticEngineApp-1.0-SNAPSHOT.jar"]
