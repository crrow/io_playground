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
use tokio::runtime::Runtime;

// Reuse configurations from blocking benchmarks
use super::blocking::{CHUNK_SIZES, MEASUREMENT_TIME, SAMPLE_SIZE, TOTAL_SIZE, WARM_UP_TIME};

criterion_group! {
		name = benches;
		config = Criterion::default()
				.sample_size(SAMPLE_SIZE)
				.warm_up_time(WARM_UP_TIME)
				.measurement_time(MEASUREMENT_TIME);
		targets = async_write, async_read
}

/// Benchmark async sequential write operations with different chunk sizes
fn async_write(c: &mut Criterion) {
	let bio = io_playground::BenchmarkIOType::Rio.new().expect("Failed to create Rio IO instance");

	let mut group = c.benchmark_group("rio::write");
	group.throughput(Throughput::Bytes(TOTAL_SIZE.as_bytes()));

	for size in CHUNK_SIZES.iter() {
		group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
			b.iter(|| {
				bio.seq_write(TOTAL_SIZE, size).expect("Failed to perform sequential write");
			});
		});
	}
	group.finish();
}

/// Benchmark async sequential read operations with different chunk sizes
fn async_read(c: &mut Criterion) {
	let bio = io_playground::BenchmarkIOType::Rio.new().expect("Failed to create Rio IO instance");

	// Prepare test data
	bio.seq_write(TOTAL_SIZE, ReadableSize::mb(1)).expect("Failed to prepare test data");

	let mut group = c.benchmark_group("rio::read");
	group.throughput(Throughput::Bytes(TOTAL_SIZE.as_bytes()));

	for size in CHUNK_SIZES.iter() {
		group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
			b.iter(|| {
				bio.seq_read(TOTAL_SIZE, size).expect("Failed to perform sequential read");
			});
		});
	}
	group.finish();
}
