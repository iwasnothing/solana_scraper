#!/bin/sh
docker-compose down
rm -Rf ../data
mkdir -p ../data
cp /root/solana_scraper/target/debug/solana_scraper .
docker build . -t solana_scraper
docker-compose up -d
docker exec $(docker ps|grep cp-kafka:latest.arm64|awk '{print $1}')  kafka-topics --bootstrap-server kafka:9092              --create              --topic transfer
