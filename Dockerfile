FROM rustlang/rust:nightly-buster-slim

WORKDIR /usr/src/oxidb
COPY . .
RUN cargo install --path .
CMD ["oxidb"]
