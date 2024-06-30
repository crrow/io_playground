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

use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

criterion_group!(benches, blocking_write);

use io_playground::{readable_size::ReadableSize, BenchmarkIO};

fn blocking_write(c: &mut Criterion) {
	let bio = io_playground::BenchmarkIOType::Blocking.new().unwrap();

	let mut group = c.benchmark_group("blocking::write");
	let total_size = ReadableSize::gb(1);
	for size in [ReadableSize::kb(4), ReadableSize::mb(1), ReadableSize::mb(4)].iter() {
		group.sample_size(10);
		group.throughput(Throughput::Bytes(total_size.as_bytes()));
		group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
			b.iter(|| {
				bio.seq_write(total_size, size).unwrap();
			});
		});
	}
	group.finish();
}
