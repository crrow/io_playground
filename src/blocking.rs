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
use rand::{prelude::SliceRandom, thread_rng};

use crate::{readable_size::ReadableSize, BenchmarkIO, BenchmarkResult, FileSystem, FileSystemOption, OperationMode};

pub struct BlockingIO {
	fs: FileSystem,
}
impl BlockingIO {
	pub fn new(fs: FileSystem) -> Self { Self { fs } }
}

impl BenchmarkIO for BlockingIO {
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let (mut file, _) = self.fs.open_file_for_reading(size.as_bytes_usize(), true)?;

		let mut buf = vec![0u8; chunk_size.as_bytes_usize()];
		let start_at = std::time::Instant::now();
		let total_cnt = size / chunk_size;
		for _ in 0..total_cnt {
			file.read_exact(&mut buf)?;
		}
		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { mode: OperationMode::SeqRead, size, chunk_size, start_at, elapsed })
	}

	fn seq_write(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult> {
		let (mut file, _) = self.fs.open_file()?;

		let start_at = std::time::Instant::now();
		let total_cnt = size / chunk_size;
		let buf = vec![1u8; chunk_size.as_bytes_usize()];
		for _ in 0..total_cnt {
			file.write(&buf)?;
		}
		let elapsed = start_at.elapsed();
		file.sync_all()?;
		Ok(BenchmarkResult { mode: OperationMode::SeqWrite, size, chunk_size, start_at, elapsed })
	}

	fn rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let (mut file, path) = self.fs.open_file_for_reading(size.as_bytes_usize(), false)?;

		// Benchmark random read
		let mut rng = thread_rng();
		let mut offsets =
			(0..size / chunk_size).into_iter().map(|e| e * chunk_size.as_bytes()).collect::<Vec<u64>>();
		offsets.shuffle(&mut rng);

		let start = std::time::Instant::now();
		let mut buffer = vec![0; chunk_size.as_bytes_usize()];
		for offset in offsets {
			file.seek(SeekFrom::Start(offset))?;
			file.read_exact(&mut buffer)?;
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
		let (file, _) = self.fs.open_file_for_reading(size.as_bytes_usize(), false)?;
		let file = Arc::new(RwLock::new(file));

		// prepare rand offset
		let mut rng = thread_rng();
		let mut offsets =
			(0..size / chunk_size).into_iter().map(|e| e * chunk_size.as_bytes()).collect::<Vec<u64>>();
		offsets.shuffle(&mut rng);
		let offsets_queue = Arc::new(crossbeam_queue::ArrayQueue::new(offsets.len()));
		for offset in offsets {
			offsets_queue.push(offset).unwrap();
		}

		let mut join_handles = Vec::with_capacity(parallelism);
		let start = std::time::Instant::now();
		for _ in 0..parallelism {
			let file_clone = Arc::clone(&file);
			let queue_clone = Arc::clone(&offsets_queue);
			join_handles.push(thread::spawn(move || {
				let mut buffer = vec![0u8; chunk_size.as_bytes_usize()];
				while let Some(offset) = queue_clone.pop() {
					let mut reader = file_clone.read().unwrap();
					reader.read_at(buffer.as_mut_slice(), offset).unwrap();
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

	fn rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		thread_cnt: u64,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
	}
}
