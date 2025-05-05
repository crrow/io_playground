dep:
	@cargo install taplo-cli --locked
	@cargo install hawkeye --locked

fmt:
	@cargo +nightly fmt --all
	@taplo format
	@taplo format --check
	@hawkeye format

# Calculate code
cloc:
	@cloc . --exclude-dir=vendor,docs,tests,examples,build,scripts,tools,target,.venv

.PHONY: c
c: check
check:
	@cargo check --all --all-features

.PHONY: b
b: bench
bench:
	@cargo bench

run:
	@cargo run --bin io_playground --release


build:
	@cargo build --bin io_playground --release

b-run-1m:
	@cargo run --release --bin io_playground bench

b-run-4kib:
	@cargo run --release --bin io_playground bench --chunk-size 4KiB

fio_sync_write:
	@fio --name=fiotest --rw=write --size=1G --bs=1m --group_reporting --ioengine=sync --direct=1

fio_sync_read:
	@fio --name=sync_read --rw=read --size=1G --bs=1m --group_reporting --ioengine=sync --direct=1

fio_sync_rand_read:
	@fio --name=sync_rand_read --rw=randread --size=1G --bs=1m --group_reporting --ioengine=sync --direct=1 --numjobs=4 --filename=sync_rand_read --offset_increment=25%

fio_sync_rand_write:
	@fio --name=sync_rand_write --rw=randwrite --size=1G --bs=1m --group_reporting --ioengine=sync --direct=1 --numjobs=4 --filename=sync_rand_write --offset_increment=25%

