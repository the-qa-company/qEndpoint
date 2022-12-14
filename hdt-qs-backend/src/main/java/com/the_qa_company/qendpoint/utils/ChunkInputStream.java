package com.the_qa_company.qendpoint.utils;

import org.apache.commons.io.file.PathUtils;
import org.rdfhdt.hdt.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * stream object to distribute a stream into chunks to read them later, will
 * delete the chunk after reading, useful for web stream parsing.
 *
 * @author Antoine Willerval
 */
public class ChunkInputStream extends InputStream {
	private static final AtomicLong ID = new AtomicLong();
	private int bufferSize = 4096;
	private int chunkSplit = 255;

	private final Path chunksLocation;
	private InputStream sourceStream;
	private final long chunkSize;
	private long currentCID = 0;
	private long maxCID = 0;
	private boolean writeEnd;
	private Throwable throwable;
	private final Object maxCidLock = new Object() {};
	private boolean end;

	private Path chunkLocation;
	private InputStream readStream;
	private final Thread thread;

	/**
	 * Create and start the worker
	 *
	 * @param chunksLocation chunk directory
	 * @param sourceStream   source stream
	 * @param chunkSize      file chunk size
	 */
	public ChunkInputStream(Path chunksLocation, InputStream sourceStream, long chunkSize) {
		this(chunksLocation, sourceStream, chunkSize, true);
	}

	/**
	 * @param chunksLocation chunk directory
	 * @param sourceStream   source stream
	 * @param chunkSize      file chunk size
	 * @param start          start the chunk input stream
	 */
	public ChunkInputStream(Path chunksLocation, InputStream sourceStream, long chunkSize, boolean start) {
		this.chunksLocation = Objects.requireNonNull(chunksLocation, "chunksLocation can't be null!");
		this.sourceStream = sourceStream;
		this.chunkSize = chunkSize;
		this.thread = new Thread(this::readStreamAsync, "ChunkInputStream#" + ID.incrementAndGet());
		if (start) {
			startWorker();
		}
	}

	/**
	 * set the chunk split
	 *
	 * @param chunkSplit chunk split
	 */
	public void setChunkSplit(int chunkSplit) {
		if (chunkSplit < 2) {
			throw new IllegalArgumentException("Chunksplit can't be lower than 2!");
		}
		this.chunkSplit = chunkSplit;
	}

	/**
	 * set the buffer size, has no effect after the worker start
	 *
	 * @param bufferSize buffer size
	 */
	public void setBufferSize(int bufferSize) {
		if (chunkSplit < 1) {
			throw new IllegalArgumentException("Buffer size can't be lower than 1");
		}
		this.bufferSize = bufferSize;
	}

	/**
	 * start the worker
	 */
	public void startWorker() {
		thread.start();
	}

	/**
	 * interrupt the worker
	 */
	public void interruptWorker() {
		thread.interrupt();
	}

	/**
	 * join the worker
	 *
	 * @throws InterruptedException worker interruption
	 */
	public void joinWorker() throws InterruptedException {
		thread.join();
	}

	private void readStreamAsync() {
		// thread run
		long chunkID = 0;
		byte[] buffer = new byte[bufferSize];

		try {
			boolean end = false;
			while (!thread.isInterrupted() && !end) {
				chunkID++;
				Path location = getChunkLocation(chunkID);
				// create parent file if required
				Files.createDirectories(location.getParent());

				long read = 0;

				try (OutputStream stream = Files.newOutputStream(location)) {

					while (read < chunkSize) {
						int toRead = (int) Math.min(buffer.length, chunkSize - read);

						int sread = sourceStream.read(buffer, 0, toRead);

						if (sread == -1) {
							// eof
							end = true;
							break;
						}

						if (sread != 0) {
							stream.write(buffer, 0, sread);
						}

						read += sread;
					}

				}

				if (read == 0) {
					// we don't add the file to the queue and we delete the
					// buffer after
					Files.deleteIfExists(location);
				} else {
					synchronized (maxCidLock) {
						maxCID = chunkID;
						maxCidLock.notifyAll();
					}
				}
			}

			if (end) {
				synchronized (maxCidLock) {
					writeEnd = true;
					maxCidLock.notifyAll();
				}
			} else {
				synchronized (maxCidLock) {
					writeEnd = true;
					throwable = new InterruptedException();
					maxCidLock.notifyAll();
				}
			}
		} catch (Throwable t) {
			synchronized (maxCidLock) {
				writeEnd = true;
				throwable = t;
				maxCidLock.notifyAll();
			}
		}
	}

	private boolean fetchStream() throws IOException {
		if (end) {
			return true;
		}
		if (readStream == null) {
			try {
				synchronized (maxCidLock) {
					while (maxCID == currentCID) {
						throwOrIo(throwable);
						if (writeEnd) {
							end = true; // no more stream
							return true;
						}
						maxCidLock.wait();
					}
				}
				currentCID++;
				chunkLocation = getChunkLocation(currentCID);
				readStream = Files.newInputStream(chunkLocation);
			} catch (InterruptedException e) {
				throw new IOException("Can't fetch next stream", e);
			}
		}
		return false;
	}

	private boolean endStream(boolean fetchAgain) throws IOException {
		try {
			if (readStream != null) {
				readStream.close();
			}
		} finally {
			readStream = null;
			try {
				if (chunkLocation != null) {
					Files.deleteIfExists(chunkLocation);
				}
			} finally {
				chunkLocation = null;
			}
		}
		return !fetchAgain || fetchStream();
	}

	/**
	 * get the chunk location from an ID
	 *
	 * @param id the ID
	 * @return chunk location
	 */
	private Path getChunkLocation(long id) {
		long c = id;

		Path file = Path.of("c_" + (c % chunkSplit) + ".bin");

		while (c > chunkSplit) {
			file = Path.of("sc_" + (c % chunkSplit)).resolve(file);
			c /= chunkSplit;
		}

		return chunksLocation.resolve(file);
	}

	@Override
	public int read() throws IOException {
		if (fetchStream()) {
			return -1;
		}
		int read;
		while ((read = readStream.read()) == -1) {
			if (endStream(true)) {
				return -1;
			}
		}

		return read;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (fetchStream()) {
			return -1;
		}
		int read;
		while ((read = readStream.read(b, off, len)) == -1) {
			if (endStream(true)) {
				return -1;
			}
		}

		return read;
	}

	@Override
	public void close() throws IOException {
		try {
			try {
				Closer.closeAll(sourceStream, (Closeable) (super::close));
			} finally {
				PathUtils.deleteDirectory(chunksLocation);
			}
		} finally {
			sourceStream = null;
			endStream(false);
		}
	}

	private static void throwOrIo(Throwable throwable) throws IOException {
		if (throwable != null) {
			if (throwable instanceof Error) {
				throw (Error) throwable;
			}
			if (throwable instanceof RuntimeException) {
				throw (RuntimeException) throwable;
			}
			if (throwable instanceof IOException) {
				throw (IOException) throwable;
			}
			throw new IOException("Read exception", throwable);
		}
	}
}
