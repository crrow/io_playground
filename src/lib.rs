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

use std::{cmp, fmt::{Display, Formatter}, fs::File, io::{BufWriter, Read, Seek, SeekFrom, Write}, os::{fd::{AsFd, AsRawFd, OwnedFd}, unix::fs::OpenOptionsExt}, path::PathBuf, sync::Arc};

use anyhow::Result;
use bon::Builder;
use clap::ValueEnum;
use rand::{RngCore, prelude::SliceRandom, rand_core::block, random};
use rustix::fs::{Mode, OFlags};
use strum_macros::{AsRefStr, EnumIter, EnumString, FromRepr};

use crate::{blocking::BlockingIO, readable_size::ReadableSize};

pub mod blocking;
pub mod build_info;
pub mod glommio;
pub mod readable_size;
pub mod rio;
pub mod tokio;

#[derive(Debug, Clone, Copy, ValueEnum, EnumString, AsRefStr, EnumIter, FromRepr)]
pub enum BenchmarkIOType {
	Blocking,
	Rio,
	Glommio,
}

impl BenchmarkIOType {
	pub const ALL: [Self; 3] = [Self::Blocking, Self::Rio, Self::Glommio];

	// pub const ALL: [Self; 2] = [Self::Blocking, Self::Rio];
	// pub const ALL: [Self; 1] = [Self::Glommio];

	pub fn new(self, opts: Options) -> Result<Box<dyn BenchmarkIO>> {
		let std_fs = FileManager::new(opts.clone())?;
		match self {
			BenchmarkIOType::Blocking => {
				let b = BlockingIO::new(std_fs);
				Ok(Box::new(b))
			}
			BenchmarkIOType::Rio => {
				let r = rio::UringRIO::new(std_fs);
				Ok(Box::new(r))
			}
			BenchmarkIOType::Glommio => Ok(Box::new(glommio::Glommio::new(std_fs))),
		}
	}
}

#[derive(Debug, Clone, Builder)]
pub struct Options {
	pub direct:  bool,
	pub dir:     PathBuf,
	pub cleanup: bool,
}

impl Default for Options {
	fn default() -> Self {
		Self {
			direct:  false,
			dir:     std::env::home_dir().unwrap().join("io_playground"),
			cleanup: true,
		}
	}
}

pub trait BenchmarkIO {
	fn seq_read(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult>;
	fn seq_write(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult>;
	fn rand_read(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult>;
	fn concurrent_rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> Result<BenchmarkResult>;
	fn concurrent_rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		parallelism: usize,
	) -> Result<BenchmarkResult>;
}

#[derive(Debug)]
pub enum OperationMode {
	SeqRead,
	SeqWrite,
	RandRead,
	ConcurrentRandRead(usize),
	ConcurrentRandWrite(usize),
}

#[derive(Debug)]
pub struct BenchmarkResult {
	pub mode:       OperationMode,
	pub size:       ReadableSize,
	pub chunk_size: ReadableSize,
	pub start_at:   std::time::Instant,
	pub elapsed:    std::time::Duration,
}

impl Display for BenchmarkResult {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		// Calculate throughput in MiB/s
		let mut megabytes_per_second = self.size.as_bytes() as u64 / self.elapsed.as_micros() as u64;
		// Round to two significant digits.
		if megabytes_per_second > 100 {
			if megabytes_per_second % 10 >= 5 {
				megabytes_per_second += 10;
			}
			megabytes_per_second = megabytes_per_second / 10 * 10;
		}

		// Calculate number of operations based on size and chunk size
		let num_operations = self.size / self.chunk_size;

		// Calculate average latency per operation
		// For concurrent operations, divide by parallelism to get per-thread latency
		let avg_latency = match self.mode {
			OperationMode::ConcurrentRandRead(p) | OperationMode::ConcurrentRandWrite(p) => {
				// For concurrent operations, calculate per-thread latency
				self.elapsed.as_micros() as u64 / (num_operations / p as u64)
			}
			_ => {
				// For sequential operations, calculate per-operation latency
				self.elapsed.as_micros() as u64 / num_operations
			}
		};

		write!(
			f,
			"mode: {:?}, size: {}, chunk_size: {}, throughput: {} MiB/s, avg_latency: {} us/op",
			self.mode, self.size, self.chunk_size, megabytes_per_second, avg_latency
		)
	}
}

pub struct FileManager {
	pub opts: Options,
}

impl FileManager {
	pub fn new(opts: Options) -> Result<Self> {
		std::fs::create_dir_all(&opts.dir)?;
		Ok(Self { opts })
	}

	pub fn open_file(&self, direct: bool) -> Result<(File, PathBuf)> {
		let path = self.generate_file_path();
		let mut opts = std::fs::OpenOptions::new();
		opts.read(true).write(true).create(true);
		if direct {
			opts.custom_flags(libc::O_DIRECT);
		}
		let file = opts.open(&path)?;

		Ok((file, path))
	}

	fn make_random_file(&self, file_size: usize) -> Result<PathBuf> {
		let path = self.generate_file_path();
		let mut file = std::fs::OpenOptions::new()
			.custom_flags(libc::O_DIRECT)
			.read(true)
			.write(true)
			.create(true)
			.open(&path)?;
		let mode = rustix::fs::FallocateFlags::empty();
		rustix::fs::fallocate(file.as_fd(), mode, 0, file_size as u64)?;
		rustix::fs::fadvise(file.as_fd(), 0, file_size as u64, rustix::fs::Advice::Sequential)?;
		let mut rng = rand::rng();

		const BLOCK_SIZE: usize = 4 << 20;
		let mut buffer = [0; BLOCK_SIZE];
		rng.fill_bytes(&mut buffer);
		let block_cnt = file_size / BLOCK_SIZE;
		for _ in 0..block_cnt {
			file.write_all(&buffer)?;
		}
		file.sync_all()?;
		Ok(path)
	}

	pub fn open_file_for_reading(
		&self,
		file_size: usize,
		seq: bool,
		direct: bool,
	) -> Result<(File, PathBuf)> {
		let path = self.make_random_file(file_size)?;
		let mut opts = std::fs::OpenOptions::new();
		opts.read(true).write(true).create(false);
		if direct {
			opts.custom_flags(libc::O_DIRECT);
		}
		let file = opts.open(&path)?;
		let advice = if seq { rustix::fs::Advice::Sequential } else { rustix::fs::Advice::Random };
		rustix::fs::fadvise(file.as_fd(), 0, file_size as u64, advice)?;
		Ok((file, path))
	}

	pub fn open_fd(&self, direct: bool) -> Result<OwnedFd> {
		let path = self.generate_file_path();
		let mut flags = OFlags::RWMODE | OFlags::CREATE;
		if direct {
			flags |= OFlags::DIRECT;
		}

		let fd = rustix::fs::open(path, flags, Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH)?;

		Ok(fd)
	}

	pub fn generate_file_path(&self) -> PathBuf {
		self.opts.dir.as_path().join(format!("file-{}", random::<u64>()))
	}

	pub fn make_random_access_offsets(size: ReadableSize, chunk_size: ReadableSize) -> Vec<u64> {
		// prepare rand offset
		let mut rng = rand::rng();
		let mut offsets =
			(0..size / chunk_size).into_iter().map(|e| e * chunk_size.as_bytes()).collect::<Vec<u64>>();
		offsets.shuffle(&mut rng);
		offsets
	}

	pub fn make_random_access_offsets_queue(
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> crossbeam_queue::ArrayQueue<u64> {
		let offsets = Self::make_random_access_offsets(size, chunk_size);
		let offsets_queue = crossbeam_queue::ArrayQueue::new(offsets.len());
		for offset in offsets {
			offsets_queue.push(offset).unwrap();
		}
		offsets_queue
	}
}

impl Drop for FileManager {
	fn drop(&mut self) {
		if self.opts.cleanup {
			std::fs::remove_dir_all(&self.opts.dir).unwrap();
		}
	}
}
