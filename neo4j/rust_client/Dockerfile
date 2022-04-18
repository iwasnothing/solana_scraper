FROM arm64v8/rust

WORKDIR /app
COPY solana_scraper .
RUN chmod u+x /app/solana_scraper
CMD /app/solana_scraper
