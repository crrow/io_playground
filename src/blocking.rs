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

use std::{io::{Read, Seek, SeekFrom, Write}, os::unix::fs::FileExt, sync::{Arc, RwLock}, thread};

use anyhow::Result;

use crate::{BenchmarkIO, BenchmarkResult, FileManager, OperationMode, Options, readable_size::ReadableSize};

pub struct BlockingIO {
	fs:   FileManager,
	opts: Options,
}
impl BlockingIO {
	pub fn new(fs: FileManager) -> Self { Self { opts: fs.opts.clone(), fs } }
}

impl BenchmarkIO for BlockingIO {
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let (mut file, _) =
			self.fs.open_file_for_reading(size.as_bytes_usize(), true, self.opts.direct)?;

		let mut buf = vec![0u8; chunk_size.as_bytes_usize()];
		let start_at = std::time::Instant::now();
		let total_cnt = size / chunk_size;
		for _ in 0..total_cnt {
			// Read exactly chunk_size bytes into the buffer
			file.read(&mut buf[0..chunk_size.as_bytes_usize()])?;
		}
		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { mode: OperationMode::SeqRead, size, chunk_size, start_at, elapsed })
	}

	fn seq_write(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult> {
		let (mut file, _) = self.fs.open_file(self.opts.direct)?;

		let start_at = std::time::Instant::now();
		let total_cnt = size / chunk_size;
		let buf = vec![1u8; chunk_size.as_bytes_usize()];
		for _ in 0..total_cnt {
			file.write(&buf[0..chunk_size.as_bytes_usize()])?;
			// sync every time just for testing how low performance it is
			file.sync_all()?;
		}
		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { mode: OperationMode::SeqWrite, size, chunk_size, start_at, elapsed })
	}

	fn rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let (mut file, path) =
			self.fs.open_file_for_reading(size.as_bytes_usize(), false, self.opts.direct)?;

		// Benchmark random read
		let offsets = FileManager::make_random_access_offsets(size, chunk_size);

		let start = std::time::Instant::now();
		let mut buffer = vec![0; chunk_size.as_bytes_usize()];
		for offset in offsets {
			file.seek(SeekFrom::Start(offset))?;
			file.read_exact(&mut buffer[0..chunk_size.as_bytes_usize()])?;
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
	) -> Result<BenchmarkResult> {
		let (file, _) =
			self.fs.open_file_for_reading(size.as_bytes_usize(), false, self.opts.direct)?;
		let file = Arc::new(RwLock::new(file));

		// prepare rand offset
		let offsets_queue = Arc::new(FileManager::make_random_access_offsets_queue(size, chunk_size));

		let mut join_handles = Vec::with_capacity(parallelism);
		let start = std::time::Instant::now();
		for _ in 0..parallelism {
			let file_clone = Arc::clone(&file);
			let queue_clone = Arc::clone(&offsets_queue);
			join_handles.push(thread::spawn(move || {
				let mut buffer = vec![0u8; chunk_size.as_bytes_usize()];
				while let Some(offset) = queue_clone.pop() {
					let mut reader = file_clone.read().unwrap();
					reader.read_at(&mut buffer[0..chunk_size.as_bytes_usize()], offset).unwrap();
				}
			}));
		}

		// Wait for all threads to finish
		for thread in join_handles {
			thread.join().unwrap();
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

	fn concurrent_rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> anyhow::Result<BenchmarkResult> {
		let (file, _) = self.fs.open_file(self.opts.direct)?;
		let file = Arc::new(RwLock::new(file));

		// Prepare random offsets for writing
		let offsets_queue = Arc::new(FileManager::make_random_access_offsets_queue(size, chunk_size));

		let mut join_handles = Vec::with_capacity(parallelism);
		let start = std::time::Instant::now();

		// Spawn threads for concurrent writing
		for _ in 0..parallelism {
			let file_clone = Arc::clone(&file);
			let queue_clone = Arc::clone(&offsets_queue);
			join_handles.push(thread::spawn(move || {
				let buf = vec![1u8; chunk_size.as_bytes_usize()];
				while let Some(offset) = queue_clone.pop() {
					let mut writer = file_clone.write().unwrap();
					writer.write_at(&buf[0..chunk_size.as_bytes_usize()], offset).unwrap();
				}
			}));
		}

		// Wait for all threads to finish
		for thread in join_handles {
			thread.join().unwrap();
		}

		// Ensure all data is written to disk
		file.write().unwrap().sync_all()?;

		let elapsed = start.elapsed();
		Ok(BenchmarkResult {
			mode: OperationMode::ConcurrentRandWrite(parallelism),
			size,
			chunk_size,
			start_at: start,
			elapsed,
		})
	}
}
