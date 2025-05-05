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

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use io_playground::{BenchmarkIO, readable_size::ReadableSize};

// Common benchmark configurations
pub const TOTAL_SIZE: ReadableSize = ReadableSize::gb(1);
pub const CHUNK_SIZES: [ReadableSize; 3] =
	[ReadableSize::kb(4), ReadableSize::mb(1), ReadableSize::mb(4)];
pub const SAMPLE_SIZE: usize = 10;
pub const WARM_UP_TIME: Duration = Duration::from_secs(1);
pub const MEASUREMENT_TIME: Duration = Duration::from_secs(3);

criterion_group! {
	name = benches;
	config = Criterion::default()
		.sample_size(SAMPLE_SIZE)
		.warm_up_time(WARM_UP_TIME)
		.measurement_time(MEASUREMENT_TIME);
	targets = blocking_seq_write, blocking_seq_read
}

/// Benchmark sequential write operations with different chunk sizes
fn blocking_seq_write(c: &mut Criterion) {
	let bio =
		io_playground::BenchmarkIOType::Blocking.new().expect("Failed to create blocking IO instance");

	let mut group = c.benchmark_group("blocking::seq_write");
	group.throughput(Throughput::Bytes(TOTAL_SIZE.as_bytes() * CHUNK_SIZES.len() as u64));

	for size in CHUNK_SIZES.iter() {
		group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
			b.iter(|| {
				bio.seq_write(TOTAL_SIZE, size).expect("Failed to perform sequential write");
			});
		});
	}
	group.finish();
}

/// Benchmark sequential read operations with different chunk sizes
fn blocking_seq_read(c: &mut Criterion) {
	let bio =
		io_playground::BenchmarkIOType::Blocking.new().expect("Failed to create blocking IO instance");

	let mut group = c.benchmark_group("blocking::seq_read");
	group.throughput(Throughput::Bytes(TOTAL_SIZE.as_bytes() * CHUNK_SIZES.len() as u64));

	for size in CHUNK_SIZES.iter() {
		group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
			b.iter(|| {
				bio.seq_read(TOTAL_SIZE, size).expect("Failed to perform sequential read");
			});
		});
	}
	group.finish();
}
