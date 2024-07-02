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

use std::{fmt::{Display, Formatter}, fs::File, os::{fd::{AsFd, AsRawFd, OwnedFd}, unix::fs::OpenOptionsExt}, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::ValueEnum;
use rand::random;
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
	// pub const ALL: [Self; 3] = [Self::Blocking, Self::Rio, Self::Glommio];
	pub const ALL: [Self; 2] = [Self::Blocking, Self::Rio];

	pub fn new(self) -> Result<Box<dyn BenchmarkIO>> {
		let opts = FileSystemOption::default();
		let std_fs = FileSystem::new(opts)?;
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
	fn rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		thread_cnt: u64,
	) -> Result<BenchmarkResult>;
}

#[derive(Debug)]
pub enum OperationMode {
	SeqRead,
	SeqWrite,
	RandRead,
	ConcurrentRandRead(usize),
	RandWrite,
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
		let mut megabytes_per_second = self.size.as_bytes() as u64 / self.elapsed.as_micros() as u64;
		// Round to two significant digits.
		if megabytes_per_second > 100 {
			if megabytes_per_second % 10 >= 5 {
				megabytes_per_second += 10;
			}
			megabytes_per_second = megabytes_per_second / 10 * 10;
		}

		write!(
			f,
			"mode: {:?}, size: {}, chunk_size: {}, throughput: {} MiB/s",
			self.mode, self.size, self.chunk_size, megabytes_per_second
		)
	}
}

#[derive(Debug, Clone)]
pub struct FileSystemOption {
	direct:     bool,
	dir:        PathBuf,
	thread_cnt: u64,
}

impl Default for FileSystemOption {
	fn default() -> Self {
		Self {
			direct:     false,
			dir:        std::env::home_dir().unwrap().join("io_playground"),
			thread_cnt: 0,
		}
	}
}

pub struct FileSystem {
	pub opts: FileSystemOption,
}

impl FileSystem {
	pub fn new(opts: FileSystemOption) -> Result<Self> {
		std::fs::create_dir_all(&opts.dir)?;
		Ok(Self { opts })
	}

	pub fn open_file(&self) -> Result<(File, PathBuf)> {
		let path = self.generate_file_path();
		let mut opts = std::fs::OpenOptions::new();
		opts.read(true).write(true).create(true);
		if self.opts.direct {
			opts.custom_flags(libc::O_DIRECT);
		};
		let file = opts.open(&path)?;

		Ok((file, path))
	}

	pub fn open_file_for_reading(&self, file_size: usize, seq: bool) -> Result<(File, PathBuf)> {
		let (file, path) = self.open_file()?;
		let mode = rustix::fs::FallocateFlags::empty();
		rustix::fs::fallocate(file.as_fd(), mode, 0, file_size as u64)?;
		let advice = if seq { rustix::fs::Advice::Sequential } else { rustix::fs::Advice::Random };
		rustix::fs::fadvise(file.as_fd(), 0, file_size as u64, advice)?;
		Ok((file, path))
	}

	pub fn open_fd(&self) -> Result<OwnedFd> {
		let path = self.generate_file_path();
		let mut flags = OFlags::RWMODE | OFlags::DIRECT | OFlags::CREATE;
		if self.opts.direct {
			flags |= OFlags::DIRECT;
		}

		let fd = rustix::fs::open(path, flags, Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH)?;

		Ok(fd)
	}

	pub fn generate_file_path(&self) -> PathBuf {
		self.opts.dir.as_path().join(format!("file-{}", random::<u64>()))
	}
}

impl Drop for FileSystem {
	fn drop(&mut self) { std::fs::remove_dir_all(&self.opts.dir).unwrap(); }
}
