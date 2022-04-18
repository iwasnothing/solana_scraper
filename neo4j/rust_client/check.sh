#!/bin/sh
list="1 2 3 4 5 6"
while true ; do
   cnt=$(docker exec $(docker ps|grep rust_client-neo4jdb-1|awk '{print $1}') bin/cypher-shell -u neo4j -p 94077079 "MATCH (a:Account)-[r:TRANSFER_TO]->(b:Account) RETURN count(*);"|grep -v count)
   lag=$(docker exec $(docker ps|grep cp-kafka:latest.arm64|awk '{print $1}') kafka-consumer-groups --bootstrap-server kafka:9092 --describe --all-groups 2>/dev/null|grep -v TOPIC|grep -v Consumer|tail -1|awk '{print $6}')
   d=$(date +%H:%M:%S)
   echo "$d,$cnt,$lag"
   sleep 60
done