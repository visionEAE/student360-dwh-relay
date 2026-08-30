# Runtime image for the Cloud Run job. The jar is built outside the image (CI or workstation),
# same convention as every service.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/student360-dwh-relay-*.jar /app/app.jar
USER 65532:65532
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
