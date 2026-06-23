FROM eclipse-temurin:11-jre-jammy

WORKDIR /app
COPY target/kubernetes-it-member.jar /app/kubernetes-it-member.jar

ENTRYPOINT ["java", "-jar", "/app/kubernetes-it-member.jar"]
