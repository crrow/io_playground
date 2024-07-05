use std::{os::fd::{AsFd, AsRawFd}, sync::Arc};

use bytes::Bytes;
use rand::{prelude::SliceRandom, thread_rng};

use crate::{readable_size::ReadableSize, BenchmarkIO, BenchmarkResult, FileSystem, OperationMode};

pub struct UringRIO {
	pub fs: FileSystem,
}

impl UringRIO {
	pub fn new(fs: FileSystem) -> Self { Self { fs } }
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
		let offsets = FileSystem::make_random_access_offsets(size, chunk_size);
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
		let (file, _) = self.fs.open_file_for_reading(size.as_bytes_usize(), false)?;
		let fd = file.as_raw_fd();
		let ring = Arc::new(rio::new()?);

		// prepare rand offset
		let offsets_queue = Arc::new(FileSystem::make_random_access_offsets_queue(size, chunk_size));
		let start = std::time::Instant::now();
		let mut handles = vec![];
		for _ in 0..parallelism {
			let offsets_queue = Arc::clone(&offsets_queue);
			let ring = ring.clone();
			let handle = std::thread::spawn(move || {
				let mut buf = vec![0; chunk_size.as_bytes_usize()];
				let mut completions = vec![];
				while let Some(offset) = offsets_queue.pop() {
					let completion = ring.read_at(&fd, &buf, offset);
					completions.push(completion);

					if completions.len() >= 128 {
						for c in completions.drain(..) {
							let cnt = c.wait().unwrap();
							if cnt < buf.len() {
								panic!("read less than expected");
							}
						}
					}
				}
				for c in completions {
					let cnt = c.wait().unwrap();
					if cnt < buf.len() {
						panic!("read less than expected");
					}
				}
			});
			handles.push(handle);
		}

		for handle in handles {
			handle.join().unwrap();
		}

		let elapsed = start.elapsed();
		Ok(BenchmarkResult {
			mode: OperationMode::ConcurrentRandRead(parallelism),
			size,
			chunk_size,
			start_at: start,
			elapsed,
		})
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
