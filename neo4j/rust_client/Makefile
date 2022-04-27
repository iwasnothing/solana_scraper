all: clean build test
clean:
	docker-compose down
	rm -f client.tar.gz
	rm -Rf ../data
	mkdir -p ../data
	echo $(shell docker image ls|grep none|awk '{print $3}'|xargs -n 1 docker image rm -f)
	echo $(shell docker volume prune -f)

build: client.tar.gz
client.tar.gz: ../../src/main.rs
	tar -zcvf client.tar.gz -C ../.. Cargo.toml src
	docker build . -t solana_scraper

test:
	docker-compose down
	rm -Rf ../data
	mkdir -p ../data
	echo $(shell docker image ls|grep none|awk '{print $3}'|xargs -n 1 docker image rm -f)
	echo $(shell docker volume prune -f)
	docker-compose up -d
	./config.sh
	./check.sh
