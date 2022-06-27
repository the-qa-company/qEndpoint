package com.the_qa_company.qendpoint.compiler;

import com.github.jsonldjava.shaded.com.google.common.base.Stopwatch;
import com.the_qa_company.qendpoint.utils.FormatUtils;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import com.the_qa_company.qendpoint.utils.rdf.BooleanQueryResult;
import com.the_qa_company.qendpoint.utils.rdf.ClosableResult;
import com.the_qa_company.qendpoint.utils.rdf.QueryResultCounter;
import com.the_qa_company.qendpoint.utils.rdf.RDFHandlerCounter;
import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.explanation.Explanation;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.server.ServerWebInputException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Repository to help using SPARQL queries on a sail repository
 *
 * @author Antoine Willerval
 */
public class SparqlRepository {
	private static final Logger logger = LoggerFactory.getLogger(SparqlRepository.class);
	private final CompiledSail compiledSail;
	private final SailRepository repository;

	public SparqlRepository(CompiledSail compiledSail) {
		this.compiledSail = Objects.requireNonNull(compiledSail, "compiledSail can't be null!");
		this.repository = new SailRepository(compiledSail);
	}

	/**
	 * @return the wrapped repository
	 */
	public SailRepository getRepository() {
		return repository;
	}

	/**
	 * Shutdown the repository
	 */
	public void shutDown() {
		repository.shutDown();
	}

	/**
	 * Init the repository
	 */
	public void init() {
		repository.init();
	}

	/**
	 * @return a connection to this repository
	 * @throws RepositoryException any exception returned by
	 *                             {@link org.eclipse.rdf4j.repository.sail.SailRepository#getConnection()}
	 */
	public SailRepositoryConnection getConnection() throws RepositoryException {
		return repository.getConnection();
	}

	/**
	 * @return the options of the compiled sail
	 */
	public CompiledSailOptions getOptions() {
		return compiledSail.getOptions();
	}

	/**
	 * reindex all the lucene sails of this repository
	 *
	 * @throws Exception any exception returned by
	 *                   {@link org.eclipse.rdf4j.sail.lucene.LuceneSail#reindex()}
	 */
	public void reindexLuceneSails() throws Exception {
		compiledSail.reindexLuceneSails();
	}

	/**
	 * @return if the sail has a least one lucene sail connected to it
	 */
	public boolean hasLuceneSail() {
		return compiledSail.hasLuceneSail();
	}

	/**
	 * execute a sparql query
	 *
	 * @param sparqlQuery  the query
	 * @param timeout      query timeout
	 * @param acceptHeader accept header
	 * @param mimeSetter   mime setter, null for no set
	 * @param out          output stream
	 * @throws java.lang.NullPointerException if an argument is null
	 */
	public void execute(String sparqlQuery, int timeout, String acceptHeader, Consumer<String> mimeSetter,
			OutputStream out) {
		execute(null, sparqlQuery, timeout, acceptHeader, mimeSetter, out);
	}

	/**
	 * execute a sparql query
	 *
	 * @param connection   the connection to use
	 * @param sparqlQuery  the query
	 * @param timeout      query timeout
	 * @param acceptHeader accept header
	 * @param mimeSetter   mime setter, null for no set
	 * @param out          output stream
	 * @throws java.lang.NullPointerException if an argument is null
	 */
	public void execute(RepositoryConnection connection, String sparqlQuery, int timeout, String acceptHeader,
			Consumer<String> mimeSetter, OutputStream out) {
		Objects.requireNonNull(sparqlQuery, "sparqlQuery can't be null");
		Objects.requireNonNull(acceptHeader, "acceptHeader can't be null");
		mimeSetter = Objects.requireNonNullElseGet(mimeSetter, () -> s -> {});
		Objects.requireNonNull(out, "output stream can't be null");

		execute0(connection, sparqlQuery, timeout, acceptHeader, mimeSetter, out);
	}

	/**
	 * execute a sparql query
	 *
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 */
	public ClosableResult<?> execute(String sparqlQuery, int timeout) {
		return execute(null, sparqlQuery, timeout);
	}

	/**
	 * execute a sparql query
	 *
	 * @param connection  the connection to use
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 */
	public ClosableResult<?> execute(RepositoryConnection connection, String sparqlQuery, int timeout) {
		return execute0(connection, sparqlQuery, timeout, null, null, null);
	}

	/**
	 * execute a sparql query
	 *
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 * @throws java.lang.IllegalArgumentException if the query isn't a tuple
	 *                                            query
	 */
	public ClosableResult<TupleQueryResult> executeTupleQuery(String sparqlQuery, int timeout) {
		return executeTupleQuery(null, sparqlQuery, timeout);
	}

	/**
	 * execute a sparql query
	 *
	 * @param connection  the connection to use
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 * @throws java.lang.IllegalArgumentException if the query isn't a tuple
	 *                                            query
	 */
	@SuppressWarnings("unchecked")
	public ClosableResult<TupleQueryResult> executeTupleQuery(RepositoryConnection connection, String sparqlQuery,
			int timeout) {
		ClosableResult<?> res = execute0(connection, sparqlQuery, timeout, null, null, null);
		assert res != null;
		if (!(res.getResult() instanceof TupleQueryResult)) {
			try {
				throw new IllegalArgumentException("the query isn't a tuple query! " + res.getResult().getClass());
			} finally {
				if (connection == null) {
					res.close();
				}
			}
		}
		return (ClosableResult<TupleQueryResult>) res;
	}

	/**
	 * execute a sparql query
	 *
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 * @throws java.lang.IllegalArgumentException               if the query
	 *                                                          isn't a boolean
	 *                                                          query
	 * @throws org.eclipse.rdf4j.repository.RepositoryException if the
	 *                                                          connection can't
	 *                                                          be closed
	 */
	public boolean executeBooleanQuery(String sparqlQuery, int timeout) {
		return executeBooleanQuery(null, sparqlQuery, timeout);
	}

	/**
	 * execute a sparql query
	 *
	 * @param connection  the connection to use
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 * @throws java.lang.IllegalArgumentException               if the query
	 *                                                          isn't a boolean
	 *                                                          query
	 * @throws org.eclipse.rdf4j.repository.RepositoryException if the
	 *                                                          connection can't
	 *                                                          be closed
	 */
	public boolean executeBooleanQuery(RepositoryConnection connection, String sparqlQuery, int timeout) {
		ClosableResult<?> res = execute0(connection, sparqlQuery, timeout, null, null, null);
		assert res != null;
		try {
			if (!(res.getResult() instanceof BooleanQueryResult)) {
				throw new IllegalArgumentException("the query isn't a boolean query! " + res.getResult().getClass());
			}
			return ((BooleanQueryResult) res.getResult()).getValue();
		} finally {
			if (connection == null) {
				// we created this connection, we can close it
				res.close();
			}
		}
	}

	/**
	 * execute a sparql query
	 *
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 * @throws java.lang.IllegalArgumentException if the query isn't a graph
	 *                                            query
	 */
	public ClosableResult<GraphQueryResult> executeGraphQuery(String sparqlQuery, int timeout) {
		return executeGraphQuery(null, sparqlQuery, timeout);
	}

	/**
	 * execute a sparql query
	 *
	 * @param connection  the connection to use
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 * @throws java.lang.IllegalArgumentException if the query isn't a graph
	 *                                            query
	 */
	@SuppressWarnings("unchecked")
	public ClosableResult<GraphQueryResult> executeGraphQuery(RepositoryConnection connection, String sparqlQuery,
			int timeout) {
		ClosableResult<?> res = execute0(connection, sparqlQuery, timeout, null, null, null);
		assert res != null;
		if (!(res.getResult() instanceof GraphQueryResult)) {
			try {
				throw new IllegalArgumentException("the query isn't a graph query! " + res.getResult().getClass());
			} finally {
				if (connection == null) {
					res.close();
				}
			}
		}
		return (ClosableResult<GraphQueryResult>) res.getResult();
	}

	/**
	 * execute a sparql query
	 *
	 * @param connection   the connection to use (null for new connection)
	 * @param sparqlQuery  the query
	 * @param timeout      query timeout
	 * @param acceptHeader accept header (useless if out is null)
	 * @param mimeSetter   mime setter (useless if out is null)
	 * @param out          output stream
	 * @return query result if the output stream is null (useless if out isn't
	 *         null), return
	 *         {@link com.the_qa_company.qendpoint.utils.rdf.BooleanQueryResult}
	 *         for boolean queries
	 */
	private ClosableResult<?> execute0(RepositoryConnection connection, String sparqlQuery, int timeout,
			String acceptHeader, Consumer<String> mimeSetter, OutputStream out) {
		connection = Objects.requireNonNullElseGet(connection, repository::getConnection);
		try {
			sparqlQuery = sparqlQuery.replaceAll("MINUS \\{(.*\\n)+.+}\\n\\s+}", "");
			// sparqlQuery = sparqlPrefixes+sparqlQuery;

			logger.info("Running given sparql query: {}", sparqlQuery);

			ParsedQuery parsedQuery = QueryParserUtil.parseQuery(QueryLanguage.SPARQL, sparqlQuery, null);

			if (compiledSail.getOptions().isDebugShowPlans()) {
				System.out.println(parsedQuery);
			}

			if (parsedQuery instanceof ParsedTupleQuery) {
				TupleQuery query = connection.prepareTupleQuery(sparqlQuery);
				query.setMaxExecutionTime(timeout);
				boolean error = false;
				try {
					if (out != null) {
						QueryResultFormat format = FormatUtils.getResultWriterFormat(acceptHeader).orElseThrow(
								() -> new ServerWebInputException("accept formats not supported: " + acceptHeader));
						mimeSetter.accept(format.getDefaultMIMEType());
						TupleQueryResultHandler writer = TupleQueryResultWriterRegistry.getInstance().get(format)
								.orElseThrow().getWriter(out);
						if (compiledSail.getOptions().isDebugShowCount()) {
							writer = new QueryResultCounter(writer);
						}
						query.evaluate(writer);
						if (compiledSail.getOptions().isDebugShowCount()) {
							logger.info("Complete query with {} triples", ((QueryResultCounter) writer).getCount());
						}
						return null;
					} else {
						return new ClosableResult<>(query.evaluate(), connection);
					}
				} catch (QueryEvaluationException q) {
					error = true;
					logger.error("This exception was caught [" + q + "]");
					q.printStackTrace();
					throw new RuntimeException(q);
				} finally {
					if (!error && compiledSail.getOptions().isDebugShowTime()) {
						System.out.println(query.explain(Explanation.Level.Timed));
					}
				}
			} else if (parsedQuery instanceof ParsedBooleanQuery) {
				BooleanQuery query = connection.prepareBooleanQuery(sparqlQuery);
				query.setMaxExecutionTime(timeout);
				if (out != null) {
					QueryResultFormat format = FormatUtils.getResultWriterFormat(acceptHeader).orElseThrow(
							() -> new ServerWebInputException("accept formats not supported: " + acceptHeader));
					mimeSetter.accept(format.getDefaultMIMEType());
					TupleQueryResultWriter writer = TupleQueryResultWriterRegistry.getInstance().get(format)
							.orElseThrow().getWriter(out);
					writer.handleBoolean(query.evaluate());
					return null;
				} else {
					return new ClosableResult<>(new BooleanQueryResult(query.evaluate()), connection);
				}
			} else if (parsedQuery instanceof ParsedGraphQuery) {
				GraphQuery query = connection.prepareGraphQuery(sparqlQuery);
				try {
					if (out != null) {
						RDFFormat format = FormatUtils.getRDFWriterFormat(acceptHeader).orElseThrow(
								() -> new ServerWebInputException("accept formats not supported: " + acceptHeader));
						mimeSetter.accept(format.getDefaultMIMEType());
						RDFHandler handler = Rio.createWriter(format, out);
						if (compiledSail.getOptions().isDebugShowCount()) {
							handler = new RDFHandlerCounter(handler);
						}
						query.evaluate(handler);
						if (compiledSail.getOptions().isDebugShowCount()) {
							logger.info("Complete query with {} triples", ((RDFHandlerCounter) handler).getCount());
						}
						return null;
					} else {
						return new ClosableResult<>(query.evaluate(), connection);
					}
				} catch (QueryEvaluationException q) {
					logger.error("This exception was caught [" + q + "]");
					q.printStackTrace();
					throw new RuntimeException(q);
				}
			} else {
				throw new ServerWebInputException("query not supported");
			}
		} finally {
			if (out != null) {
				connection.close();
			}
		}
	}

	/**
	 * execute a sparql update query
	 *
	 * @param sparqlQuery the query
	 * @param timeout     query timeout
	 * @param out         the output stream, can be null
	 */
	public void executeUpdate(String sparqlQuery, int timeout, OutputStream out) {
		// logger.info("Running update query:"+sparqlQuery);
		sparqlQuery = Pattern.compile("MINUS \\{(?s).*?}\\n {2}}").matcher(sparqlQuery).replaceAll("");
		try (SailRepositoryConnection connection = repository.getConnection()) {
			connection.setParserConfig(new ParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false));

			Update preparedUpdate = connection.prepareUpdate(QueryLanguage.SPARQL, sparqlQuery);
			preparedUpdate.setMaxExecutionTime(timeout);

			Stopwatch stopwatch = Stopwatch.createStarted();
			preparedUpdate.execute();
			stopwatch.stop(); // optional
			logger.info("Time elapsed to execute update query: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
			if (out != null) {
				try (JsonGenerator gen = Json.createGenerator(out)) {
					gen.writeStartObject().write("ok", true).writeEnd();
				}
			}
		}
	}

	/**
	 * load a file using updates, will split the file into split of size
	 * {@link CompiledSailOptions#getRdf4jSplitUpdate()}
	 *
	 * @param inputStream file stream
	 * @param filename    file name to get the compression and the rdf type
	 * @throws IOException parsing exception
	 */
	public void loadFile(InputStream inputStream, String filename) throws IOException {
		StopWatch timeWatch = new StopWatch();

		// uncompress the file if required
		InputStream fileStream = RDFStreamUtils.uncompressedStream(inputStream, filename);
		// get a triple iterator for this stream
		Iterator<Statement> it = RDFStreamUtils.readRDFStreamAsIterator(fileStream,
				Rio.getParserFormatForFileName(filename)
						.orElseThrow(() -> new ServerWebInputException("file format not supported " + filename)),
				true);

		long triples = 0;
		long total = 0;
		while (it.hasNext()) {
			int updates = getOptions().getRdf4jSplitUpdate();
			try (RepositoryConnection connection = getConnection()) {
				connection.begin();
				for (int i = 0; i < updates; i++) {
					Statement stmt = it.next();
					connection.add(stmt);
					triples++;
					if (!it.hasNext()) {
						break;
					}
				}
				connection.commit();
			}
			if (triples >= 100_000L) {
				total += triples;
				logger.info("loaded {} triples (+{}), {}", total, triples, timeWatch.stopAndShow());
				triples = 0;
			}
		}
		total += triples;
		logger.info("loaded {} triples (+{})", total, triples);

		logger.info("NT file loaded in {}", timeWatch.stopAndShow());
	}
}
