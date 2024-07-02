use std::{io::{Read, Seek, SeekFrom}, time::Duration};

use bytes::Bytes;
use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::{io::{DmaFile, DmaStreamReaderBuilder, DmaStreamWriterBuilder}, LocalExecutorBuilder, Placement};
use rand::{prelude::SliceRandom, thread_rng};

use crate::{readable_size::ReadableSize, BenchmarkIO, BenchmarkResult, FileSystem, OperationMode};

pub struct Glommio {
	pub fs: FileSystem,
}

impl Glommio {
	pub fn new(fs: FileSystem) -> Self { Self { fs } }
}

impl BenchmarkIO for Glommio {
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let (file, path) = self.fs.open_file_for_reading(size.as_bytes_usize(), true)?;
		drop(file);
		let mut buf = vec![0u8; chunk_size.as_bytes_usize()];
		let mut bytes_read = 0;
		let mut ops = 0;

		let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
			.spin_before_park(Duration::from_millis(10))
			.spawn(move || async move {
				let file = DmaFile::open(&path).await.unwrap();
				let mut stream = DmaStreamReaderBuilder::new(file)
					.with_read_ahead(0)
					.with_buffer_size(chunk_size.as_bytes_usize())
					.build();

				let start_at = std::time::Instant::now();
				loop {
					let res = stream.read(&mut buf).await.unwrap();
					bytes_read += res;
					ops += 1;
					if res == 0 {
						break;
					}
				}
				stream.close().await.unwrap();
				assert_eq!(bytes_read, size.as_bytes_usize());
				BenchmarkResult {
					mode: OperationMode::SeqRead,
					size,
					chunk_size,
					start_at,
					elapsed: start_at.elapsed(),
				}
			})
			.unwrap();

		let br = local_ex.join().unwrap();

		Ok(br)
	}

	fn seq_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let path = self.fs.generate_file_path();
		let contents = Bytes::from(vec![1u8; chunk_size.as_bytes_usize()]);
		let write_cnt = size / chunk_size;

		let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
			.spin_before_park(Duration::from_millis(10))
			.spawn(move || async move {
				let file = DmaFile::create(&path).await.unwrap();
				let mut stream = DmaStreamWriterBuilder::new(file)
					.with_write_behind(0)
					.with_buffer_size(chunk_size.as_bytes_usize())
					.build();

				let start_at = std::time::Instant::now();
				for _ in 0..write_cnt {
					stream.write_all(contents.as_ref()).await.unwrap();
				}
				stream.close().await.unwrap();
				BenchmarkResult {
					mode: OperationMode::SeqWrite,
					size,
					chunk_size,
					start_at,
					elapsed: start_at.elapsed(),
				}
			})
			.unwrap();
		let br = local_ex.join().unwrap();
		Ok(br)
	}

	fn rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
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
