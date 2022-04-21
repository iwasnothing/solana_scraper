clean:
	docker-compose down
	rm -f client.tar.gz
	rm -Rf ../data
	mkdir -p ../data
	docker image ls|grep none|awk '{print $3}'|xargs -n 1 docker image rm -f
	docker volume prune -f

build: client.tar.gz
client.tar.gz: ../../src/main.rs
	tar -zcvf client.tar.gz -C ../.. Cargo.toml src
	docker build . -t solana_scraper

test:
	docker-compose up -d
	docker exec $(docker ps|grep cp-kafka|awk '{print $1}')  kafka-topics --bootstrap-server kafka:9092              --create              --topic transfer
	docker exec $(docker ps|grep rust_client_neo4jdb_1|awk '{print $1}') bin/cypher-shell -u neo4j -p 94077079 "CREATE CONSTRAINT unique_account_key FOR (a:Account) REQUIRE a.key IS UNIQUE"
	sleep 10
	./check.sh
