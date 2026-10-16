FROM artifactory.jfrog.sbi:443/cbops-docker-images/alpine_java:22-jdk

# Set working directory inside container
#WORKDIR /app

ARG JAR_FILE=target/*.jar
# Set environment variable (optional)
ENV SPRING_PROFILES_ACTIVE=dev
ENV SERVER_PORT=8091
ENV TZ=Asia/Kolkata

# Copy JAR file into the container
COPY ${JAR_FILE} app.jar

EXPOSE 8091

# Run app
CMD ["java", "-Duser.timezone=Asia/Kolkata","-jar", "app.jar"]