# Clean up
run `make clean` to clean up the database and docker image

# Build Rust client
run `make build` to compile the rust program and build its docker image

# run test
run `make test` to setup docker containers for Rust program, kafka, and neo4j db by docker-compose, and run the checking script forever.

The sample output is like:
> Creating network "rust_client_default" with the default driver
> Creating rust_client_neo4jdb_1   ... done
> Creating rust_client_zookeeper_1 ... done
> Creating rust_client_kafka_1     ... done
> Creating rust_client_solana_scraper_1 ... done
> sleep 10
> docker exec $(docker ps|grep cp-kafka|awk '{print $1}')  kafka-topics --bootstrap-server kafka:9092              --create              --topic transfer
> Created topic transfer.
> docker exec $(docker ps|grep rust_client_neo4jdb_1|awk '{print $1}') bin/cypher-shell -u neo4j -p 94077079 "CREATE CONSTRAINT unique_account_key FOR (a:Account) REQUIRE a.key IS UNIQUE"
> sleep 10
> ./check.sh
> 16:39:25,0,68,,       8
> 16:40:33,2258,6,2022-04-22T08:38:43,       8

The above csv are "Current time, total transfer stored in DB, Kafka consumer lag, the latest datetime of the transfer in DB, the total no. of DB connection.