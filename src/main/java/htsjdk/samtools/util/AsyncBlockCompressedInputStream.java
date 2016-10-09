/*
 * The MIT License
 *
 * Copyright (c) 2016 Daniel Cameron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

import htsjdk.samtools.Defaults;
import htsjdk.samtools.seekablestream.SeekableStream;

/**
 * Asynchronous read-ahead implementation of
 * {@link htsjdk.samtools.util.BlockCompressedInputStream}.
 * 
 * Note that this implementation is not synchronized. If multiple threads access
 * an instance concurrently, it must be synchronized externally.
 */
public class AsyncBlockCompressedInputStream
		extends BlockCompressedInputStream {
	private static final int READ_AHEAD_BUFFERS = (int) Math
			.ceil(Defaults.NON_ZERO_BUFFER_SIZE
					/ BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE);

	private final BlockingQueue<byte[]> freeBuffers = new ArrayBlockingQueue<>(
			READ_AHEAD_BUFFERS);

	private BlockingQueue<CompletableFuture<DecompressedBlock>> mResult = new ArrayBlockingQueue<>(
			READ_AHEAD_BUFFERS);

	public AsyncBlockCompressedInputStream(final InputStream stream) {
		super(stream, true);
	}

	public AsyncBlockCompressedInputStream(final File file) throws IOException {
		super(file);
	}

	public AsyncBlockCompressedInputStream(final URL url) {
		super(url);
	}

	public AsyncBlockCompressedInputStream(final SeekableStream strm) {
		super(strm);
	}

	@Override
	protected DecompressedBlock nextBlock(byte[] bufferAvailableForReuse) {
		if (bufferAvailableForReuse != null) {
			freeBuffers.offer(bufferAvailableForReuse);
		}
		return nextBlockSync();
	}

	Supplier<DecompressedBlock> fetchNextBlock = () -> {
		return processNextBlock(freeBuffers.poll());
	};

	SerialExecutor service = new SerialExecutor(ForkJoinPool.commonPool());

	Runnable orderNextBlock = () -> {
		synchronized (service) {
			if (mResult.remainingCapacity() == 0) {
				return;
			}
			mResult.offer(
					CompletableFuture.supplyAsync(fetchNextBlock, service));
		}
	};

	@Override
	protected void prepareForSeek() {
		synchronized (service) {
			mResult.clear();
			service.clear();
			super.prepareForSeek();
		}
	}

	private DecompressedBlock nextBlockSync() {

		CompletableFuture<DecompressedBlock> nextBlockFuture;

		synchronized (service) {
			while ((nextBlockFuture = mResult.poll()) == null) {
				orderNextBlock.run();
			}
		}

		nextBlockFuture.thenRunAsync(orderNextBlock, service);
		return nextBlockFuture.join();

	}

	@Override
	public void close() throws IOException {
		synchronized (service) {
			mResult.clear();
			service.clear();
			super.close();
		}
	}

}

class SerialExecutor implements Executor {
	final Queue<Runnable> tasks = new ArrayDeque<Runnable>();
	final Executor executor;
	Runnable active;

	SerialExecutor(Executor executor) {
		this.executor = executor;
	}

	public synchronized void execute(final Runnable r) {
		tasks.offer(new Runnable() {
			public void run() {
				try {
					r.run();
				} finally {
					scheduleNext();
				}
			}
		});
		if (active == null) {
			scheduleNext();
		}
	}

	synchronized public void clear() {
		tasks.clear();
		active = null;
	}

	protected synchronized void scheduleNext() {
		if ((active = tasks.poll()) != null) {
			executor.execute(active);
		}
	}
}