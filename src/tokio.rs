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

use std::{io::Cursor, os::fd::AsRawFd, path::Path, sync::Arc};

use anyhow::Result;
use tokio::{io::{AsyncSeekExt, AsyncWriteExt}, sync::Mutex};

pub fn benchmark_write<P: AsRef<Path>>(path: P, size: u64, chunk_size: u64) -> Result<()> {
	let path = path.as_ref();
	let total_cnt = size / chunk_size;
	let buf = bytes::Bytes::from(vec![1u8; chunk_size as usize]);

	let runtime =
		tokio::runtime::Builder::new_multi_thread().enable_io().worker_threads(2).build()?;

	let file = runtime.block_on(async {
		let file = tokio::fs::OpenOptions::new().write(true).create(true).open(path).await?;
		Ok::<tokio::fs::File, std::io::Error>(file)
	})?;
	let file = Arc::new(Mutex::new(file));

	let mut handlers = Vec::with_capacity(total_cnt as usize);
	for i in 0..total_cnt {
		let offset = i * chunk_size;
		let buf = buf.clone();
		let file = file.clone();
		let task = runtime.spawn(async move {
			let mut writer = file.lock().await;
			writer.seek(tokio::io::SeekFrom::Start(offset)).await?;
			writer.write_all(&buf).await?;
			Ok::<(), std::io::Error>(())
		});
		handlers.push(task);
	}

	runtime.block_on(async {
		futures::future::try_join_all(handlers).await?;
		Ok::<(), std::io::Error>(())
	})?;
	Ok(())
}

pub fn benchmark_read<P: AsRef<Path>>(path: P, size: u64, chunk_size: u64) -> Result<()> { todo!() }
