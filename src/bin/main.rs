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

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use io_playground::{BenchmarkIOType, Options, build_info, readable_size::ReadableSize};

#[derive(Debug, Parser)]
#[clap(
	name = "io_playground",
	about= "A simple playground for io operations",
	author = build_info::AUTHOR,
	version = build_info::FULL_VERSION)]
struct Cli {
	#[command(subcommand)]
	commands: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
	Bench(BenchArgs),
	Read(ReadArgs),
	Write(WriteArgs),
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
#[command(long_about = r"

Do read test.
Examples:

io_playground read
")]
struct ReadArgs;

impl ReadArgs {
	fn run(&self) -> Result<()> { Ok(()) }
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
#[command(long_about = r"

Do write test.
Examples:

io_playground write
")]
struct WriteArgs {
	/// The directory to store the file
	#[arg(short, long, group = "input", default_value = "io_playground")]
	dir:     PathBuf,
	/// Whether to use direct I/O
	#[arg(short, long, group = "input", default_value = "true")]
	direct:  bool,
	/// Whether to cleanup the file after the test
	#[arg(long, group = "input", default_value = "false")]
	cleanup: bool,

	/// The size of the file to write
	#[arg(short, long, group = "input", default_value = "1GiB")]
	size:       ReadableSize,
	/// The size of the chunk to write
	#[arg(short, long, group = "input", default_value = "1MiB")]
	chunk_size: ReadableSize,
	/// The mode of the benchmark
	#[arg(long, short, group = "input", value_enum, default_value = "rio")]
	mode:       BenchmarkIOType,
}

impl From<&WriteArgs> for Options {
	fn from(args: &WriteArgs) -> Self {
		Options { direct: args.direct, dir: args.dir.clone(), cleanup: args.cleanup }
	}
}

impl WriteArgs {
	fn run(self) -> Result<()> {
		let harness = self.mode.new(Options::from(&self))?;
		let br = harness.seq_write(self.size, self.chunk_size)?;
		println!("{}", br);
		Ok(())
	}
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
#[command(long_about = r"

Run all test at the same time.
Examples:

io_playground bench
")]
struct BenchArgs {
	/// The directory to store the file
	#[arg(short, long, group = "input", default_value = "io_playground")]
	dir:         PathBuf,
	/// Whether to use direct I/O
	#[arg(short, long, group = "input", default_value = "true")]
	direct:      bool,
	/// Whether to cleanup the file after the test
	#[arg(short, long, group = "input", default_value = "true")]
	cleanup:     bool,
	/// The size of the file to write
	#[arg(short, long, group = "input", default_value = "1GiB")]
	size:        ReadableSize,
	/// The size of the chunk to write
	#[arg(short, long, group = "input", default_value = "1MiB")]
	chunk_size:  ReadableSize,
	/// The number of threads to use
	#[arg(short, long, group = "input", default_value = "4")]
	parallelism: usize,
}

impl From<&BenchArgs> for Options {
	fn from(args: &BenchArgs) -> Self {
		Options { direct: args.direct, dir: args.dir.clone(), cleanup: args.cleanup }
	}
}

impl BenchArgs {
	fn run(self) -> Result<()> {
		for mode in BenchmarkIOType::ALL {
			let mode_name = mode.as_ref();
			let harness = mode.new(Options::from(&self))?;
			let br = harness.seq_write(self.size, self.chunk_size)?;
			println!("{mode_name}, {}", br);
			let br = harness.seq_read(self.size, self.chunk_size)?;
			println!("{mode_name}, {}", br);
			let br = harness.rand_read(self.size, self.chunk_size)?;
			println!("{mode_name}, {}", br);
			// let br = harness.concurrent_rand_read(self.size, self.chunk_size,
			// self.parallelism)?; println!("{mode_name}, {}", br);
			// let br = harness.concurrent_rand_write(self.size, self.chunk_size,
			// self.parallelism)?; println!("{mode_name}, {}", br);
		}
		Ok(())
	}
}

fn main() -> Result<()> {
	let cli = Cli::parse();
	match cli.commands {
		Commands::Write(wa) => wa.run(),
		Commands::Bench(ba) => ba.run(),
		_ => {
			todo!()
		}
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	#[test]
	fn basic() {
		let v = Duration::from_millis(100);
		println!("{:?}", ReadableSize::gb(1) / v.as_millis() as u64 * 1000)
	}
}
