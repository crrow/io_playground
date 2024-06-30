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

use std::io::Write;

use anyhow::Result;

use crate::{readable_size::ReadableSize, BaseIODrive, BenchmarkIO, BenchmarkResult, IoOpts, OperationKind};

pub struct BlockingIO(pub BaseIODrive);

impl BenchmarkIO for BlockingIO {
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
	}

	fn seq_write(&self, size: ReadableSize, chunk_size: ReadableSize) -> Result<BenchmarkResult> {
		let mut file = self.0.open_file()?;

		let start_at = std::time::Instant::now();
		let total_cnt = size / chunk_size;
		let buf = vec![1u8; chunk_size.as_bytes_usize()];
		for _ in 0..total_cnt {
			file.write(&buf)?;
		}
		let elapsed = start_at.elapsed();
		Ok(BenchmarkResult { kind: OperationKind::SeqWrite, size, chunk_size, start_at, elapsed })
	}

	fn rand_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
		thread_cnt: u64,
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
