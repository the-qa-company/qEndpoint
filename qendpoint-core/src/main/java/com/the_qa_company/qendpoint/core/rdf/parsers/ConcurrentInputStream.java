package com.the_qa_company.qendpoint.core.rdf.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;

public class ConcurrentInputStream {

	private static final Logger log = LoggerFactory.getLogger(ConcurrentInputStream.class);
	private final InputStream source;
	private final int numberOfStreams;

	private PipedInputStream[] pipedInputStreams;
	private PipedOutputStream[] pipedOutputStreams;

	private PipedInputStream bnodeInputStream;
	private PipedOutputStream bnodeOutputStream;

	private Thread readerThread;

	public ConcurrentInputStream(InputStream stream, int numberOfStreams) {
		this.source = stream;
		this.numberOfStreams = numberOfStreams;
		setupPipes();
		startReadingThread();
	}

	private void setupPipes() {
		pipedInputStreams = new PipedInputStream[numberOfStreams];
		pipedOutputStreams = new PipedOutputStream[numberOfStreams];

		// The size of the pipes needs to be larger than the buffer of the
		// buffered reader that Jena uses inside the parser, which is 131072
		// bytes. If our pipeSize is too small it limits the ability for the
		// parsers to work concurrently.
		int pipeSize = 131072 * 1024;

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

	private void startReadingThread() {
		readerThread = new Thread(new ReaderThread());

		readerThread.setName("ConcurrentInputStream reader");
		readerThread.setDaemon(true);
		readerThread.start();
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

	private class ReaderThread implements Runnable {
		@Override
		public void run() {
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
			} catch (IOException e) {
				log.error("Error reading input stream", e);
				// If there's a read error, close everything.
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
	}
}
