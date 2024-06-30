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

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use io_playground::{build_info, readable_size::ReadableSize, BenchmarkIOType};

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
	fn run(&self) -> Result<()> {
		Ok(())
	}
}

#[derive(Debug, Clone, Args)]
#[command(flatten_help = true)]
#[command(long_about = r"

Do write test.
Examples:

io_playground write
")]
struct WriteArgs{
	size: usize,
	chunk_size: usize,
	mode: BenchmarkIOType,
}

impl WriteArgs {
	fn run(self) -> Result<()> {
		let cap = ReadableSize::gb(1);
		let chunk_size = ReadableSize::mb(1);

		let hairness = self.mode.new();

		io_playground::tokio::benchmark_write(path, cap.as_bytes(), chunk_size.as_bytes())?;
		let elapsed = start.elapsed();

		println!("write {:?}, chunk_size {:?}, elapsed: {:?} ", cap, chunk_size, elapsed,);
		Ok(())
	}
}

fn main() -> Result<()> {
	let cli = Cli::parse();
	match cli.commands {
		Commands::Blocking(ba) => ba.run(),
		Commands::Tokio(ta) => ta.run(),
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
