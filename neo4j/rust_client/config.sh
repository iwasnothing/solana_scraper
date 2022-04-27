#!/bin/sh

keyword1="kafka.server.FinalizedFeatureCache"
keyword2="INFO  Started"
n1=0
n2=0
while [[ $n2 -eq 0 ]] ; do
    n2=$(docker logs $(docker ps|grep rust_client_neo4jdb_1|awk '{print $1}') | grep -c "$keyword2")
done
docker exec $(docker ps|grep rust_client_neo4jdb_1|awk '{print $1}') bin/cypher-shell -u neo4j -p 94077079 "CREATE CONSTRAINT unique_account_key FOR (a:Account) REQUIRE a.key IS UNIQUE"
while [[ $n1 -eq 0 ]] ; do
    n1=$(docker logs $(docker ps|grep cp-kafka|awk '{print $1}') |grep -c $keyword1)
done
docker exec $(docker ps|grep cp-kafka|awk '{print $1}')  kafka-topics --bootstrap-server kafka:9092              --create              --topic transfer
