dep:
	@cargo install taplo-cli --locked
	@cargo install hawkeye --locked

fmt:
	@cargo +nightly fmt --all
	@taplo format
	@taplo format --check
	@hawkeye format
	@buf format -w

.PHONY: c
c: check
check:
	@cargo check --all --all-features

.PHONY: b
b: bench
bench:
	@cargo bench

build:
	@cargo build --bin io_playground --release

b-run:
	@cargo run --release --bin io_playground blocking

t-run:
	@cargo run --release --bin io_playground tokio

fio_sync_write:
	@fio --name=fiotest --rw=write --size=1G --bs=1m --group_reporting --ioengine=sync
