// Copyright 2024 Crrow <hahadaxigua@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright Crrow <hahadaxigua@gmail.com> and the IO Playground contributors
// SPDX-License-Identifier: Apache-2.0

use std::{io::{Read, Seek, SeekFrom}, rc::Rc, sync::Arc, time::Duration};

use bytes::{Bytes, buf::Reader};
use futures::{AsyncReadExt, AsyncWriteExt};
use glommio::{LocalExecutorBuilder, Placement, io::{DmaFile, DmaStreamReaderBuilder, DmaStreamWriterBuilder}};

use crate::{BenchmarkIO, BenchmarkResult, FileManager, OperationMode, Options, readable_size::ReadableSize};

pub struct Glommio {
	pub fs:   FileManager,
	pub opts: Options,
}

impl Glommio {
	pub fn new(fs: FileManager) -> Self { Self { opts: fs.opts.clone(), fs } }
}

impl BenchmarkIO for Glommio {
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let (file, path) =
			self.fs.open_file_for_reading(size.as_bytes_usize(), true, self.opts.direct)?;
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
		let (file, path) =
			self.fs.open_file_for_reading(size.as_bytes_usize(), false, self.opts.direct)?;
		let offsets = FileManager::make_random_access_offsets(size, chunk_size);
		drop(file);

		let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
			.spin_before_park(Duration::from_millis(10))
			.spawn(move || async move {
				let file = Rc::new(DmaFile::open(&path).await.unwrap());
				let time = std::time::Instant::now();

				for offset in offsets {
					let file = file.clone();
					glommio::spawn_local(async move {
						file.read_at(offset, chunk_size.as_bytes_usize()).await.unwrap();
					})
					.await;
				}

				BenchmarkResult {
					mode: OperationMode::RandRead,
					size,
					chunk_size,
					start_at: time,
					elapsed: time.elapsed(),
				}
			})
			.unwrap();
		let r = local_ex.join().unwrap();
		Ok(r)
	}

	fn concurrent_rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
		// let (file, path) = self.fs.open_file_for_reading(size.as_bytes_usize(),
		// false)?; let offsets = FileSystem::make_random_access_offsets(size,
		// chunk_size); drop(file);
		// let offsets_queue =
		// Arc::new(FileSystem::make_random_access_offsets_queue(size, chunk_size));
		//
		// let local_ex = LocalExecutorBuilder::new(Placement::Fixed(0))
		// 	.spin_before_park(Duration::from_millis(10))
		// 	.spawn(move || async move {
		// 		let file = Rc::new(DmaFile::open(&path));
		// 		let time = std::time::Instant::now();
		// 		let mut tasks = Vec::new();
		//
		// 		for _ in 0..parallelism {
		// 			tasks.push(
		// 				glommio::spawn_local(enclose! { (file) async move {
		// 							let mut expected =
		// Vec::with_capacity(chunk_size.as_bytes_usize()); 							while let
		// Some(offset) = offsets_queue.pop(){ 								file.read(offset,
		// chunk_size.as_bytes_usize(), &expected).await.unwrap(); 							}
		// 				}})
		// 				.detach(),
		// 			);
		// 		}
		// 		for task in tasks {
		// 			task.await;
		// 		}
		// 		BenchmarkResult {
		// 			mode: OperationMode::ConcurrentRandRead(parallelism),
		// 			size,
		// 			chunk_size,
		// 			start_at: time,
		// 			elapsed: time.elapsed(),
		// 		}
		// 	})
		// 	.unwrap();
		// let r = local_ex.join().unwrap();
		// Ok(r)
	}

	fn concurrent_rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
	}
}
