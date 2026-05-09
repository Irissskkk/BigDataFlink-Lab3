#!/bin/bash
cd flink-job
mvn clean package
cp target/flink-star-transformer-1.0.jar ../
echo "JAR file built successfully!"
