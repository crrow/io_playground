use std::os::fd::{AsFd, AsRawFd};

use bytes::Bytes;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use crate::{readable_size::ReadableSize, BenchmarkIO, BenchmarkResult, FileSystem, OperationMode};

pub struct UringRIO {
	pub fs: FileSystem,
}

impl UringRIO {
	pub fn new(fs: FileSystem) -> Self { Self { fs: fs } }
}

impl BenchmarkIO for UringRIO {
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let ring = rio::new()?;
		let (file, _) = self.fs.open_file_for_reading(size.as_bytes_usize(), true)?;
		let fd = file.as_raw_fd();
		let total_cnt = size / chunk_size;
		let buf = vec![1u8; chunk_size.as_bytes_usize()];
		let start_at = std::time::Instant::now();

		for i in 0..total_cnt {
			let completion = ring.read_at(&fd, &buf, i * chunk_size.as_bytes());
			let cnt = completion.wait()?;
			if cnt < buf.len() {
				return Err(anyhow::anyhow!("read less than expected"));
			}
		}

		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { mode: OperationMode::SeqRead, size, chunk_size, start_at, elapsed })
	}

	fn seq_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let ring = rio::new()?;
		let (file, _) = self.fs.open_file()?;
		let fd = file.as_raw_fd();

		let buf = Bytes::from(vec![1u8; chunk_size.as_bytes_usize()]);

		let start_at = std::time::Instant::now();
		let total_cnt = size / chunk_size;

		for i in 0..total_cnt {
			let completion = ring.write_at(&fd, &buf, i * chunk_size.as_bytes());
			let cnt = completion.wait()?;
			if cnt < buf.len() {
				return Err(anyhow::anyhow!("write less than expected"));
			}
		}
		let completion = ring.fsync(&file);
		completion.wait()?;

		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { mode: OperationMode::SeqWrite, size, chunk_size, start_at, elapsed })
	}

	fn rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let (mut file, path) = self.fs.open_file_for_reading(size.as_bytes_usize(), false)?;
		let ring = rio::new()?;
		let fd = file.as_raw_fd();
		// Benchmark random read
		let mut rng = thread_rng();
		let mut offsets =
			(0..size / chunk_size).into_iter().map(|e| e * chunk_size.as_bytes()).collect::<Vec<u64>>();
		offsets.shuffle(&mut rng);

		let mut buf = vec![0; chunk_size.as_bytes_usize()];
		let start = std::time::Instant::now();
		for offset in offsets {
			let completion = ring.read_at(&fd, &buf, offset);
			let cnt = completion.wait()?;
			if cnt < buf.len() {
				return Err(anyhow::anyhow!("write less than expected"));
			}
		}
		let elapsed = start.elapsed();
		Ok(BenchmarkResult {
			mode: OperationMode::RandRead,
			size,
			chunk_size,
			start_at: start,
			elapsed,
		})
	}

	fn concurrent_rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
	}

	fn rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		thread_cnt: u64,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
	}
}
