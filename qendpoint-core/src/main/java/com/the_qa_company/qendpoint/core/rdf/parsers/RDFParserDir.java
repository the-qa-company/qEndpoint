package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.util.ContainerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * @author Antoine Willerval
 */
public class RDFParserDir implements RDFParserCallback {
	private static final Logger log = LoggerFactory.getLogger(RDFParserDir.class);
	private final HDTOptions spec;
	final int async;

	public RDFParserDir(HDTOptions spec) {
		this.spec = spec;
		long dirAsyncValue = spec.getInt(HDTOptionsKeys.ASYNC_DIR_PARSER_KEY, 1);
		if (dirAsyncValue == 0) {
			// use processor count for 0 to be full parallel
			async = Runtime.getRuntime().availableProcessors();
		} else if (dirAsyncValue < 0 || dirAsyncValue >= Integer.MAX_VALUE - 5) {
			throw new IllegalArgumentException(
					"Invalid value for " + HDTOptionsKeys.ASYNC_DIR_PARSER_KEY + ": " + dirAsyncValue);
		} else {
			async = (int) dirAsyncValue;
		}

	}

	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		doParse(Path.of(fileName), baseUri, notation, keepBNode, callback);
	}

	@Override
	public void doParse(Path path, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		if (notation != RDFNotation.DIR) {
			throw new IllegalArgumentException("Can't parse notation different than " + RDFNotation.DIR + "!");
		}

		try {
			if (async == 1) {
				// no async parser, faster to use recursion
				try (Stream<Path> subFiles = Files.list(path)) {
					subFiles.forEach(child -> {
						try {
							if (Files.isDirectory(child)) {
								doParse(child, baseUri, RDFNotation.DIR, keepBNode, callback);
								return;
							}
							RDFParserCallback rdfParserCallback;
							RDFNotation childNotation;
							try {
								// get the notation of the file
								childNotation = RDFNotation.guess(child.toFile());
								rdfParserCallback = RDFParserFactory.getParserCallback(childNotation, spec);
							} catch (IllegalArgumentException e) {
								log.warn("Ignore file {}", child, e);
								return;
							}
							log.debug("parse {}", child);
							// we can parse it, parsing it
							rdfParserCallback.doParse(child.toAbsolutePath().toString(), baseUri, childNotation,
									keepBNode, callback);
						} catch (ParserException e) {
							throw new ContainerException(e);
						}
					});
				} catch (IOException | SecurityException e) {
					throw new ParserException(e);
				} catch (ContainerException e) {
					throw (ParserException) e.getCause();
				}
			} else {
				// use async parser because we will need to call it from
				// multiple
				// threads
				RDFCallback asyncRdfCallback = callback.async();
				// create the pool
				ExecutorService executorService = Executors.newFixedThreadPool(async);
				// list of all the future loaded by the parser
				FutureList list = new FutureList();
				// send the first task with the root directory
				list.add(executorService.submit(
						new LoadTask(executorService, path, baseUri, RDFNotation.DIR, keepBNode, asyncRdfCallback)));

				// wait for end of all the futures
				try {
					list.await();
				} catch (ExecutionException e) {
					throw new ParserException(e.getCause());
				} catch (InterruptedException e) {
					throw new ParserException(e);
				} finally {
					// close the service
					executorService.shutdown();
				}
			}
		} catch (InvalidPathException e) {
			throw new ParserException(e);
		}
	}

	@Override
	public void doParse(InputStream in, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback,
			boolean parallel) throws ParserException {
		throw new NotImplementedException("Can't parse a stream of directory!");
	}

	private static class FutureList {
		private final List<Future<FutureList>> list;

		private FutureList() {
			this.list = new ArrayList<>();
		}

		public void add(Future<FutureList> e) {
			list.add(e);
		}

		/**
		 * await all the futures
		 *
		 * @throws ExecutionException   exception while running the method
		 * @throws InterruptedException interruption of the thread
		 */
		public void await() throws ExecutionException, InterruptedException {
			while (!list.isEmpty()) {
				Future<FutureList> future = list.remove(list.size() - 1);

				try {
					// get the next futures
					mergeWith(future.get());
				} catch (InterruptedException e) {
					if (Thread.interrupted()) {
						throw e;
					}
				}
			}
		}

		/**
		 * merge this list with another one
		 *
		 * @param other other
		 */
		public void mergeWith(FutureList other) {
			this.list.addAll(other.list);
		}
	}

	private class LoadTask implements Callable<FutureList> {
		private final ExecutorService executorService;
		private final Path path;
		private final String baseUri;
		private final RDFNotation notation;
		private final boolean keepBNode;
		private final RDFCallback callback;

		private LoadTask(ExecutorService executorService, Path path, String baseUri, RDFNotation notation,
				boolean keepBNode, RDFCallback callback) {
			this.executorService = executorService;
			this.path = path;
			this.baseUri = baseUri;
			this.notation = notation;
			this.keepBNode = keepBNode;
			this.callback = callback;
		}

		@Override
		public FutureList call() throws ParserException {
			FutureList list = new FutureList();
			if (notation == RDFNotation.DIR) {
				try (Stream<Path> subFiles = Files.list(path)) {
					subFiles.forEach(child -> {
						if (Files.isDirectory(child)) {
							list.add(executorService.submit(new LoadTask(executorService, child, baseUri,
									RDFNotation.DIR, keepBNode, callback)));
							return;
						}
						RDFNotation childNotation;
						try {
							// get the notation of the file
							childNotation = RDFNotation.guess(child.toFile());
						} catch (IllegalArgumentException e) {
							log.warn("Ignore file {}", child, e);
							return;
						}
						log.debug("parse {}", child);
						// we can parse it, parsing it
						list.add(executorService.submit(
								new LoadTask(executorService, child, baseUri, childNotation, keepBNode, callback)));
					});
				} catch (IOException | SecurityException e) {
					throw new ParserException(e);
				} catch (ContainerException e) {
					throw (ParserException) e.getCause();
				}
			} else {
				RDFParserCallback rdfParserCallback;
				try {
					// get the parser for the file
					rdfParserCallback = RDFParserFactory.getParserCallback(notation, spec);
				} catch (IllegalArgumentException | NotImplementedException e) {
					log.warn("Ignore file {}", path, e);
					return list;
				}

				log.debug("parse {}", path);
				rdfParserCallback.doParse(path.toAbsolutePath().toString(), baseUri, notation, keepBNode, callback);
			}

			return list;
		}
	}
}
