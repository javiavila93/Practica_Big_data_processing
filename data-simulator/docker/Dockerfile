FROM openjdk:8-slim-buster
ENV KAFKA_SERVERS localhost:9092
COPY data-simulator-assembly-0.1.jar /
COPY data_simulator.sh /
CMD ./data_simulator.sh