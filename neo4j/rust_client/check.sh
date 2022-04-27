#!/bin/sh
list="1 2 3 4 5 6"
while true ; do
   cnt=$(docker exec $(docker ps|grep rust_client_neo4jdb_1|awk '{print $1}') bin/cypher-shell -u neo4j -p 94077079 "MATCH (a:Account)-[r:TRANSFER_TO]->(b:Account) RETURN count(*);"|grep -v count)
   dt=$(docker exec $(docker ps|grep rust_client_neo4jdb_1|awk '{print $1}') bin/cypher-shell -u neo4j -p 94077079 "MATCH (a:Account)-[r:TRANSFER_TO]->(b:Account) RETURN r.datetime as time ORDER BY time DESC LIMIT 1 ;"|grep -v time|awk -FT '{print $2}')
   conn=$(docker exec $(docker ps|grep rust_client_neo4jdb_1|awk '{print $1}') bin/cypher-shell -u neo4j -p 94077079 "CALL dbms.listConnections"|wc -l)
   lag=$(docker exec $(docker ps|grep cp-kafka|awk '{print $1}') kafka-consumer-groups --bootstrap-server kafka:9092 --describe --all-groups 2>/dev/null|grep -v TOPIC|grep -v Consumer|tail -1|awk '{print $6}')
   d=$(TZ=UTC date +%H:%M:%S)
   echo "$d,$cnt,$lag,$dt,$conn"
   sleep 60
done
