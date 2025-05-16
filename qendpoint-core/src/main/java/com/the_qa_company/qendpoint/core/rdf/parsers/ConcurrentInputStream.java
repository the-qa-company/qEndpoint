package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;

public class ConcurrentInputStream implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(ConcurrentInputStream.class);
	private final InputStream source;
	private final int numberOfStreams;

	private PipedInputStream[] pipedInputStreams;
	private PipedOutputStream[] pipedOutputStreams;

	private PipedInputStream bnodeInputStream;
	private PipedOutputStream bnodeOutputStream;

	private ExceptionThread readerThread;

	public ConcurrentInputStream(InputStream stream, int numberOfStreams) {
		this.source = stream;
		this.numberOfStreams = numberOfStreams;
		setupPipes();
	}

	private void setupPipes() {
		pipedInputStreams = new PipedInputStream[numberOfStreams];
		pipedOutputStreams = new PipedOutputStream[numberOfStreams];

		// The size of the pipes needs to be larger than the buffer of the
		// buffered reader that Jena uses inside the parser, which is 131072
		// bytes. If our pipeSize is too small it limits the ability for the
		// parsers to work concurrently.
		int pipeSize = 0x8_000_000;

		try {
			// Set up main fan-out pipes
			for (int i = 0; i < numberOfStreams; i++) {
				pipedOutputStreams[i] = new PipedOutputStream();
				pipedInputStreams[i] = new PipedInputStream(pipedOutputStreams[i], pipeSize);
			}

			// Set up bnode pipe
			bnodeOutputStream = new PipedOutputStream();
			bnodeInputStream = new PipedInputStream(bnodeOutputStream, pipeSize);

		} catch (IOException e) {
			throw new RuntimeException("Error creating pipes", e);
		}
	}

	public ExceptionThread getReadingThread() {
		if (readerThread == null) {
			readerThread = new ExceptionThread(this::readerThreadRun, "ConcurrentInputStream reader");
			readerThread.setDaemon(true);
		}
		return readerThread;
	}

	/**
	 * Returns the stream for blank-node lines only.
	 */
	public InputStream getBnodeStream() {
		return bnodeInputStream;
	}

	/**
	 * Returns the array of InputStreams that share all concurrently read data.
	 */
	public InputStream[] getStreams() {
		return pipedInputStreams;
	}

	private void readerThreadRun() throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(source, StandardCharsets.UTF_8))) {
			String line;
			int currentStreamIndex = 0;
			while ((line = reader.readLine()) != null) {
				if (line.isEmpty()) {
					continue; // Skip empty lines
				}

				byte[] data = (line + "\n").getBytes(StandardCharsets.UTF_8);

				if (line.contains("_:")) {
					// Write to bnodeOutputStream only
					bnodeOutputStream.write(data);
				} else {
					// Write to a single stream from pipedOutputStreams in a
					// round-robin manner
					pipedOutputStreams[currentStreamIndex].write(data);
					currentStreamIndex = (currentStreamIndex + 1) % pipedOutputStreams.length;
				}
			}
		} finally {
			// Close all output streams to signal EOF
			for (PipedOutputStream out : pipedOutputStreams) {
				try {
					out.close();
				} catch (IOException ignored) {
				}
			}

			try {
				bnodeOutputStream.close();
			} catch (IOException e) {
				log.error("Error closing bnodeOutputStream", e);
			}
		}
	}

	@Override
	public void close() throws ExceptionThread.ExceptionThreadException {
		try {
			try {
				Closer.closeAll(pipedOutputStreams, bnodeOutputStream);
			} catch (IOException e) {
				throw new ExceptionThread.ExceptionThreadException(e);
			}
		} finally {
			if (readerThread != null) {
				try {
					readerThread.interrupt();
					readerThread.joinAndCrashIfRequired();
				} catch (InterruptedException e) {
					// ignore interruption because we asked it
				}
			}
		}
	}
}
