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

use std::{fs::File, os::{fd::OwnedFd, unix::fs::OpenOptionsExt}, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::ValueEnum;
use rand::random;
use rustix::fs::{Mode, OFlags};

use crate::{blocking::BlockingIO, readable_size::ReadableSize};

pub mod blocking;
pub mod build_info;
pub mod readable_size;
pub mod rio;
pub mod tokio;

#[derive(Debug, Clone, ValueEnum)]
pub enum BenchmarkIOType {
	Blocking,
	Tokio,
	Rio,
}

impl BenchmarkIOType {
	pub fn new(self) -> Result<Box<dyn BenchmarkIO>> {
		let opts = IoOpts::default();
		let base_io_drive = BaseIODrive::new(opts)?;
		match self {
			BenchmarkIOType::Blocking => {
				let b = BlockingIO(base_io_drive);
				Ok(Box::new(b))
			}
			BenchmarkIOType::Tokio => todo!(),
			BenchmarkIOType::Rio => {
				let r = rio::UringRIO::new(base_io_drive);
				Ok(Box::new(r))
			}
		}
	}
}

pub trait BenchmarkIO {
	fn seq_read(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult>;
	fn seq_write(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult>;
	fn rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		thread_cnt: u64,
	) -> Result<BenchmarkResult>;
	fn rand_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		thread_cnt: u64,
	) -> Result<BenchmarkResult>;
}

#[derive(Debug)]
pub enum OperationKind {
	SeqRead,
	SeqWrite,
	RandSeq,
	RandWrite,
}

#[derive(Debug)]
pub struct BenchmarkResult {
	pub kind:       OperationKind,
	pub size:       ReadableSize,
	pub chunk_size: ReadableSize,
	pub start_at:   std::time::Instant,
	pub elapsed:    std::time::Duration,
}

#[derive(Debug, Clone)]
pub struct IoOpts {
	direct:     bool,
	dir:        PathBuf,
	thread_cnt: u64,
}

impl Default for IoOpts {
	fn default() -> Self {
		Self {
			direct:     false,
			dir:        std::env::home_dir().unwrap().join("io_playground"),
			thread_cnt: 0,
		}
	}
}

pub struct BaseIODrive {
	pub opts: IoOpts,
}

impl BaseIODrive {
	pub fn new(opts: IoOpts) -> Result<Self> {
		std::fs::create_dir_all(&opts.dir)?;
		Ok(Self { opts })
	}

	pub fn open_file(&self) -> Result<File> {
		let path = self.opts.dir.as_path().join(format!("file-{}", random::<u64>()));
		let mut opts = std::fs::OpenOptions::new();
		opts.read(true).write(true).create(true);
		if self.opts.direct {
			opts.custom_flags(libc::O_DIRECT);
		};
		let file = opts.open(&path)?;
		Ok(file)
	}

	pub fn open_fd(&self) -> Result<OwnedFd> {
		let path = self.opts.dir.as_path().join(format!("file-{}", random::<u64>()));

		let mut flags = OFlags::RWMODE | OFlags::DIRECT | OFlags::CREATE;
		if self.opts.direct {
			flags |= OFlags::DIRECT;
		}

		let fd = rustix::fs::open(path, flags, Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH)?;

		Ok(fd)
	}
}

impl Drop for BaseIODrive {
	fn drop(&mut self) { std::fs::remove_dir_all(&self.opts.dir).unwrap(); }
}
