use bytes::Bytes;

use crate::{readable_size::ReadableSize, BaseIODrive, BenchmarkIO, BenchmarkResult, IoOpts, OperationKind};

pub struct UringRIO {
	pub inner: BaseIODrive,
}

impl BenchmarkIO for UringRIO {
	fn seq_read(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		todo!()
	}

	fn seq_write(
		&self,
		size: ReadableSize,
		chunk_size: ReadableSize,
	) -> anyhow::Result<BenchmarkResult> {
		let ring = rio::new()?;
		let fd = self.inner.open_fd()?;

		let buf = Bytes::from(vec![1u8; chunk_size.as_bytes_usize()]);

		let start_at = std::time::Instant::now();
		let total_cnt = size / chunk_size;

		let mut completions = Vec::with_capacity(total_cnt as usize);
		for _ in 0..total_cnt {
			let completion = ring.write_at(&fd, &buf, 0);
			completions.push(completion);
		}

		for c in completions {
			c.wait()?;
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
