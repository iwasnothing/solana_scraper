FROM rust

RUN cargo new --bin solana_scraper
WORKDIR /solana_scraper
COPY client.tar.gz .
RUN gzip -cd client.tar.gz |tar -xvf -
RUN cargo build --release
RUN ls target/release
CMD target/release/solana_scraper
