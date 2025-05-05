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

use std::{os::fd::{AsFd, AsRawFd}, sync::{Arc, atomic::AtomicBool}};

use bytes::Bytes;
use crossbeam_channel::{Select, select};
use rand::{prelude::SliceRandom, thread_rng};

use crate::{BenchmarkIO, BenchmarkResult, FileManager, OperationMode, readable_size::ReadableSize};

/// UringRIO implements I/O operations using Linux's io_uring interface.
///
/// io_uring is a modern Linux I/O interface that provides:
/// - Asynchronous I/O operations with minimal system calls
/// - Batching of I/O operations for better performance
/// - Direct I/O support for bypassing the page cache
/// - Efficient submission and completion queues
///
/// This implementation uses the `rio` crate which provides a safe Rust
/// interface to io_uring.
pub struct UringRIO {
	pub fs: FileManager,
}

impl UringRIO {
	pub fn new(fs: FileManager) -> Self { Self { fs } }
}

impl BenchmarkIO for UringRIO {
	/// Performs sequential read operations using io_uring.
	///
	/// This implementation:
	/// 1. Creates a new io_uring instance
	/// 2. Opens a file with sequential access hints
	/// 3. Reads data in chunks sequentially
	/// 4. Uses read_at for direct I/O operations
	/// 5. Verifies each read operation completes successfully
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		// Create a new io_uring instance for this operation
		let ring = rio::new()?;
		// Open file with sequential access hints for better performance
		let (file, _) = self.fs.open_file_for_reading(size.as_bytes_usize(), true, true)?;
		let fd = file.as_raw_fd();
		// Calculate total number of chunks to read
		let total_cnt = size / chunk_size;
		// Create a buffer for reading data
		let buf = vec![1u8; chunk_size.as_bytes_usize()];
		let start_at = std::time::Instant::now();

		// Read data sequentially in chunks
		for i in 0..total_cnt {
			// Submit read operation to io_uring
			let completion = ring.read_at(&fd, &buf, i * chunk_size.as_bytes());
			// Wait for completion and verify bytes read
			let cnt = completion.wait()?;
			if cnt < buf.len() {
				return Err(anyhow::anyhow!("read less than expected"));
			}
		}

		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { mode: OperationMode::SeqRead, size, chunk_size, start_at, elapsed })
	}

	/// Performs sequential write operations using io_uring.
	///
	/// This implementation:
	/// 1. Creates a new io_uring instance
	/// 2. Opens a file for writing
	/// 3. Writes data in chunks sequentially
	/// 4. Uses write_at for direct I/O operations
	/// 5. Verifies each write operation completes successfully
	/// 6. Syncs the file to ensure data is written to disk
	fn seq_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		// Create a new io_uring instance for this operation
		let ring = rio::new()?;
		// Open file for writing
		let (file, _) = self.fs.open_file(true)?;
		let fd = file.as_raw_fd();

		// Create a buffer filled with 1s for writing
		let buf = Bytes::from(vec![1u8; chunk_size.as_bytes_usize()]);

		let start_at = std::time::Instant::now();
		// Calculate total number of chunks to write
		let total_cnt = size / chunk_size;
		for i in 0..total_cnt {
			// Submit write operation to io_uring
			let completion =
				ring.write_at_ordered(&fd, &buf, i * chunk_size.as_bytes(), rio::Ordering::Link);
			completion.wait()?;
			let sync_completion = ring.fsync_ordered(&file, rio::Ordering::Link);
			sync_completion.wait()?;
		}

		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { mode: OperationMode::SeqWrite, size, chunk_size, start_at, elapsed })
	}

	/// Performs random read operations using io_uring.
	///
	/// This implementation:
	/// 1. Creates a new io_uring instance
	/// 2. Opens a file with random access hints
	/// 3. Generates random offsets for reading
	/// 4. Uses read_at for direct I/O operations at random positions
	/// 5. Verifies each read operation completes successfully
	fn rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		// Open file with random access hints for better performance
		let (mut file, path) = self.fs.open_file_for_reading(size.as_bytes_usize(), false, true)?;
		// Create a new io_uring instance for this operation
		let ring = rio::new()?;
		let fd = file.as_raw_fd();
		// Generate random offsets for reading
		let offsets = FileManager::make_random_access_offsets(size, chunk_size);
		// Create a buffer for reading data
		let mut buf = vec![0; chunk_size.as_bytes_usize()];
		let start = std::time::Instant::now();

		// Read data at random offsets
		for offset in offsets {
			// Submit read operation to io_uring at random offset
			let completion = ring.read_at(&fd, &buf, offset);
			// Wait for completion and verify bytes read
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

	/// Performs concurrent random read operations using io_uring.
	///
	/// This implementation:
	/// 1. Creates a shared io_uring instance using Arc
	/// 2. Opens a file with random access hints
	/// 3. Creates a thread-safe queue of random offsets
	/// 4. Spawns multiple threads for concurrent reading
	/// 5. Each thread:
	///    - Pops offsets from the queue
	///    - Uses read_at for direct I/O operations
	///    - Processes completions in batches of 128 to avoid overwhelming the
	///      ring
	/// 6. Verifies all read operations complete successfully
	fn concurrent_rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> anyhow::Result<BenchmarkResult> {
		// Open file with random access hints
		let (file, _) = self.fs.open_file_for_reading(size.as_bytes_usize(), false, true)?;
		let fd = file.as_raw_fd();
		// Create a shared io_uring instance for concurrent operations
		let ring = Arc::new(rio::new()?);

		// Create a thread-safe queue of random offsets
		let offsets_queue = Arc::new(FileManager::make_random_access_offsets_queue(size, chunk_size));
		let start = std::time::Instant::now();
		let mut handles = vec![];
		// Spawn worker threads for concurrent reading
		for _ in 0..parallelism {
			let offsets_queue = Arc::clone(&offsets_queue);
			let ring = ring.clone();
			let handle = std::thread::spawn(move || {
				// Create a buffer for reading data
				let mut buf = vec![0; chunk_size.as_bytes_usize()];
				let mut completions = vec![];
				// Process offsets from the queue
				while let Some(offset) = offsets_queue.pop() {
					// Submit read operation to io_uring
					let completion = ring.read_at(&fd, &buf, offset);
					completions.push(completion);

					// Process completions in batches to avoid overwhelming the ring
					if completions.len() >= 128 {
						for c in completions.drain(..) {
							let cnt = c.wait().unwrap();
							if cnt < buf.len() {
								panic!("read less than expected");
							}
						}
					}
				}
				// Process any remaining completions
				for c in completions {
					let cnt = c.wait().unwrap();
					if cnt < buf.len() {
						panic!("read less than expected");
					}
				}
			});
			handles.push(handle);
		}

		// Wait for all threads to complete
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

	/// Performs concurrent random write operations using io_uring.
	///
	/// This implementation:
	/// 1. Creates a shared io_uring instance using Arc
	/// 2. Opens a file for writing
	/// 3. Creates a thread-safe queue of random offsets
	/// 4. Creates a shared buffer filled with 1s and wraps it in Arc for
	///    thread-safe sharing
	/// 5. Spawns multiple threads for concurrent writing
	/// 6. Each thread:
	///    - Pops offsets from the queue
	///    - Uses write_at for direct I/O operations at random positions
	///    - Processes completions in batches of 128 to avoid overwhelming the
	///      ring
	///    - Verifies each write operation completes successfully
	/// 7. Waits for all threads to complete
	/// 8. Syncs the file to ensure all data is written to disk
	///
	/// The implementation uses several optimizations:
	/// - Batch processing of completions to avoid overwhelming the io_uring ring
	/// - Thread-safe sharing of the write buffer using Arc
	/// - Direct I/O operations using write_at
	/// - Concurrent execution using multiple threads
	fn concurrent_rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> anyhow::Result<BenchmarkResult> {
		// Open file for writing
		let (file, _) = self.fs.open_file(true)?;
		let fd = file.as_raw_fd();
		// Create a shared io_uring instance for concurrent operations
		let ring = Arc::new(rio::new()?);

		// Create a thread-safe queue of random offsets
		let offsets_queue = Arc::new(FileManager::make_random_access_offsets_queue(size, chunk_size));
		// Create a shared buffer filled with 1s for writing
		let buf = Arc::new(Bytes::from(vec![1u8; chunk_size.as_bytes_usize()]));

		let start = std::time::Instant::now();
		let mut handles = Vec::with_capacity(parallelism);

		// Spawn worker threads for concurrent writing
		for _ in 0..parallelism {
			let offsets_queue = Arc::clone(&offsets_queue);
			let ring = ring.clone();
			let buf = Arc::clone(&buf);
			let handle = std::thread::spawn(move || {
				let mut completions = Vec::new();
				// Process offsets from the queue
				while let Some(offset) = offsets_queue.pop() {
					// Submit write operation to io_uring
					let completion = ring.write_at(&fd, &*buf, offset);
					completions.push(completion);

					// Process completions in batches to avoid overwhelming the ring
					if completions.len() >= 128 {
						for c in completions.drain(..) {
							let cnt = c.wait().unwrap();
							if cnt < buf.len() {
								panic!("write less than expected");
							}
						}
					}
				}

				// Process any remaining completions
				for c in completions {
					let cnt = c.wait().unwrap();
					if cnt < buf.len() {
						panic!("write less than expected");
					}
				}
			});
			handles.push(handle);
		}

		// Wait for all threads to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// Ensure all data is written to disk
		let completion = ring.fsync(&file);
		completion.wait()?;

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
