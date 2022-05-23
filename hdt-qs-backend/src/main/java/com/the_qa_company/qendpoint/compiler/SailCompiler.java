package com.the_qa_company.qendpoint.compiler;

import com.the_qa_company.qendpoint.compiler.sail.FilterLinkedSailCompiler;
import com.the_qa_company.qendpoint.compiler.sail.LinkedSailCompiler;
import com.the_qa_company.qendpoint.compiler.sail.LinkedSailLinkedSailCompiler;
import com.the_qa_company.qendpoint.compiler.sail.LuceneSailCompiler;
import com.the_qa_company.qendpoint.compiler.sail.MultiFilterLinkedSailCompiler;
import com.the_qa_company.qendpoint.compiler.source.EmptyTripleSourceGetter;
import com.the_qa_company.qendpoint.compiler.source.ModelTripleSourceGetter;
import com.the_qa_company.qendpoint.compiler.source.SailTripleSourceModel;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class to build a sail from an RDF config file
 *
 * @author Antoine Willerval
 */
public class SailCompiler {
	private static final Logger logger = LoggerFactory.getLogger(SailCompiler.class);
	private static final Pattern DIR_OPT = Pattern.compile("\\$\\{([^}]+)}");

	/**
	 * convert a {@link org.eclipse.rdf4j.model.Value} to an
	 * {@link org.eclipse.rdf4j.model.IRI}
	 *
	 * @param value the value
	 * @return the iri
	 * @throws SailCompilerException if this value isn't of the right type
	 */
	public static IRI asIRI(Value value) throws SailCompilerException {
		if (value.isIRI()) {
			return (IRI) value;
		}
		throw new SailCompilerException(value + " can't be converted to an IRI!");
	}

	/**
	 * convert a {@link org.eclipse.rdf4j.model.Value} to a
	 * {@link org.eclipse.rdf4j.model.Resource}
	 *
	 * @param value the value
	 * @return the resource
	 * @throws SailCompilerException if this value isn't of the right type
	 */
	public static Resource asResource(Value value) throws SailCompilerException {
		if (value.isResource()) {
			return (Resource) value;
		}
		throw new SailCompilerException(value + " can't be converted to a Resource!");
	}

	private TripleSourceModel store = new EmptyTripleSourceGetter();
	private final Map<IRI, LinkedSailCompiler> compilers = new HashMap<>();
	private final Map<String, String> dirStrings = new HashMap<>();

	/**
	 * create a sail with default LinkedSailCompilers
	 */
	public SailCompiler() {
		registerCustomCompiler(new FilterLinkedSailCompiler());
		registerCustomCompiler(new LinkedSailLinkedSailCompiler());
		registerCustomCompiler(new LuceneSailCompiler());
		registerCustomCompiler(new MultiFilterLinkedSailCompiler());
	}

	/**
	 * load a RDF file describing the model into a new store
	 *
	 * @param stream the stream to the rdf file
	 * @param format the format of the file
	 * @throws IOException can't read the stream
	 */
	public void load(InputStream stream, RDFFormat format) throws IOException {
		load(Rio.parse(stream, format));
	}

	/**
	 * load a sail describing the model
	 *
	 * @param store the store to fetch the model information
	 */
	public void load(Sail store) {
		setStore(new SailTripleSourceModel(store));
	}

	/**
	 * load a sail describing the model
	 *
	 * @param store the store to fetch the model information
	 */
	public void load(Model store) {
		setStore(new ModelTripleSourceGetter(store));
	}

	/**
	 * load a RDF file describing the model into a new store
	 *
	 * @param rdfFile the rdf file
	 * @throws IOException can't read the stream
	 */
	public void load(Path rdfFile) throws IOException {
		RDFFormat format = Rio.getParserFormatForFileName(rdfFile.getFileName().toString())
				.orElseThrow(() -> new IllegalArgumentException("Can't find parser for file: " + rdfFile));

		try (InputStream stream = new BufferedInputStream(Files.newInputStream(rdfFile))) {
			load(stream, format);
		}
	}

	/**
	 * set this store to compile the model
	 *
	 * @param store the store
	 */
	public void setStore(TripleSourceModel store) {
		this.store = store;
	}

	/**
	 * register a {@link SailCompilerSchema#PARSED_STRING_DATATYPE} parsed value
	 *
	 * @param name  value key
	 * @param value value
	 */
	public void registerDirString(String name, String value) {
		if (!DIR_OPT.matcher("${" + name + "}").matches()) {
			throw new IllegalArgumentException("Dir key should respect the pattern " + DIR_OPT);
		}
		dirStrings.put(name, value);
	}

	/**
	 * load every method annotated with {@link ParsedStringValue} in an object.
	 *
	 * @param object the object to read
	 */
	public void registerDirObject(Object object) {
		for (Method m : object.getClass().getDeclaredMethods()) {
			ParsedStringValue value = m.getAnnotation(ParsedStringValue.class);
			if (value != null) {
				try {
					if (m.getParameterCount() != 0) {
						throw new IllegalArgumentException(
								"The count of parameters on the method " + m.getName() + "isn't 0");
					}
					String key = value.value();
					String val = String.valueOf(m.invoke(object));
					logger.debug("registering dir value: {}={}", key, val);
					registerDirString(key, val);
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw new IllegalArgumentException("Can't read the value of the method " + m.getName(), e);
				}
			}
		}
	}

	/**
	 * convert a {@link org.eclipse.rdf4j.model.Value} to a String
	 *
	 * @param value the value
	 * @return the string
	 * @throws SailCompilerException if this value isn't of the right type
	 */
	public String asLitString(Value value) throws SailCompilerException {
		if (value.isLiteral()) {
			Literal lit = (Literal) value;
			String label = lit.getLabel();
			if (SailCompilerSchema.PARSED_STRING_DATATYPE.equals(lit.getDatatype())) {
				return parseDir(label);
			} else {
				return label;
			}
		}
		throw new SailCompilerException(value + " can't be converted to a Literal!");
	}

	/**
	 * parse a directory and add {@literal ${key}} into value from
	 * {@link #registerDirString(String, String)}
	 *
	 * @param dir the directory string to parse
	 * @return the parsed dir
	 * @throws SailCompilerException if a value can't be found
	 */
	public String parseDir(String dir) throws SailCompilerException {
		Matcher matcher = DIR_OPT.matcher(dir);

		StringBuilder s = new StringBuilder();
		int last = 0;

		while (matcher.find()) {
			int start = matcher.start();
			if (last != start) {
				s.append(dir, last, start);
			}

			String key = matcher.group(1);
			String value = dirStrings.get(key);
			if (value == null) {
				throw new SailCompilerException("Can't find ${" + key + "}");
			}

			s.append(value);
			last = matcher.end();
		}

		if (last != dir.length()) {
			s.append(dir, last, dir.length());
		}

		return s.toString();
	}

	/**
	 * register a LinkedSail sub sail
	 *
	 * @param compiler the sail
	 */
	public void registerCustomCompiler(LinkedSailCompiler compiler) {
		compilers.put(compiler.getIri(), compiler);
	}

	/**
	 * get a sail for a particular IRI
	 *
	 * @param iri the type iri
	 * @return the sail
	 * @throws SailCompilerException if the sail can't be found
	 */
	public LinkedSailCompiler getCompiler(IRI iri) throws SailCompilerException {
		LinkedSailCompiler compiler = compilers.get(iri);
		if (compiler == null) {
			throw new SailCompilerException("Can't find a sail for the name " + iri);
		}
		return compiler;
	}

	/**
	 * compile the read file from a source, if no main node is defined, the
	 * source is returned
	 *
	 * @param source the triple source to pipe to the model
	 * @return the sail for this model piped to the source or the source if no
	 *         main node is described
	 * @throws SailCompilerException sail error
	 */
	public NotifyingSail compile(NotifyingSail source) throws SailCompilerException {
		LinkedSail<? extends NotifyingSail> sail;
		try (SailCompilerReader reader = new SailCompilerReader()) {
			// read parsedString properties
			reader.search(SailCompilerSchema.MAIN, SailCompilerSchema.PARSED_STRING_PARAM).stream()
					.map(SailCompiler::asResource).forEach(rnode -> {
						String key = asLitString(reader.searchOne(rnode, SailCompilerSchema.PARAM_KEY));
						String value = asLitString(reader.searchOne(rnode, SailCompilerSchema.PARAM_VALUE));
						dirStrings.put(key, value);
					});

			Optional<Value> node = reader.searchOneOpt(SailCompilerSchema.MAIN, SailCompilerSchema.NODE);
			if (node.isEmpty()) {
				return source;
			}

			sail = reader.compileNode(node.get());
		}

		sail.getSailConsumer().accept(source);
		return sail.getSail();
	}

	/**
	 * @return a reader
	 */
	public SailCompilerReader getReader() {
		return new SailCompilerReader();
	}

	/**
	 * A exception linked with the model compilation
	 *
	 * @author Antoine Willerval
	 */
	public static class SailCompilerException extends RuntimeException {
		public SailCompilerException(String message) {
			super(message);
		}

		public SailCompilerException(Throwable cause) {
			super(cause);
		}

		public SailCompilerException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	/**
	 * Reader to read the nodes
	 *
	 * @author Antoine Willerval
	 */
	public class SailCompilerReader implements AutoCloseable {
		private final TripleSourceGetter connection;

		private SailCompilerReader() throws SailCompilerException {
			if (store == null) {
				throw new SailCompilerException("No store defined!");
			}
			connection = store.getGetter();
		}

		/**
		 * @return the sail sail linked with this reader
		 */
		public SailCompiler getSailCompiler() {
			return SailCompiler.this;
		}

		/**
		 * construct a node from a value
		 *
		 * @param node the describing node
		 * @return the sail
		 * @throws SailCompilerException sail error
		 */
		public LinkedSail<? extends NotifyingSail> compileNode(Value node) throws SailCompilerException {
			Resource rnode = asResource(node);
			IRI type = asIRI(searchOne(rnode, SailCompilerSchema.TYPE));
			return getCompiler(type).compileWithParam(this, rnode);
		}

		/**
		 * search for exactly one statement with this subject and predicate
		 *
		 * @param subject   the subject
		 * @param predicate the predicate
		 * @return the value
		 * @throws SailCompilerException can't find the triple
		 */
		public Value searchOne(Resource subject, IRI predicate) throws SailCompilerException {
			Value out;
			try (CloseableIteration<? extends Statement, SailCompilerException> it = connection.getStatements(subject,
					predicate, null)) {
				if (!it.hasNext()) {
					throw new SailCompilerException(
							"Can't find statements for the query (" + subject + ", " + predicate + ", ???)!");
				}
				out = it.next().getObject();
				if (it.hasNext()) {
					throw new SailCompilerException(
							"Too many value for the query (" + subject + ", " + predicate + ", ???)!");
				}
			}
			return out;
		}

		/**
		 * search for exactly one or zero statement with this subject and
		 * predicate
		 *
		 * @param subject   the subject
		 * @param predicate the predicate
		 * @return the value
		 * @throws SailCompilerException can't find the triple
		 */
		public Optional<Value> searchOneOpt(Resource subject, IRI predicate) throws SailCompilerException {
			Value out;
			try (CloseableIteration<? extends Statement, SailCompilerException> it = connection.getStatements(subject,
					predicate, null)) {
				if (!it.hasNext()) {
					return Optional.empty();
				}
				out = it.next().getObject();
				if (it.hasNext()) {
					throw new SailCompilerException(
							"Too many value for the query (" + subject + ", " + predicate + ", ???)!");
				}
			}
			return Optional.of(out);
		}

		/**
		 * search for a property value in the graph
		 *
		 * @param subject  the subject to read
		 * @param property the property to read and check values
		 * @return the value
		 * @throws SailCompilerException if the value isn't a valid value for
		 *                               this property
		 */
		public <T> T searchPropertyValue(Resource subject,
				SailCompilerSchema.Property<T, ? extends SailCompilerSchema.ValueHandler<T>> property)
				throws SailCompilerException {
			return searchOneOpt(subject, property.getIri()).map(property::throwIfNotValidValue)
					.orElseGet(property.getHandler()::defaultValue);
		}

		/**
		 * search for the statements with this subject and predicate
		 *
		 * @param subject   the subject
		 * @param predicate the predicate
		 * @return the values
		 */
		public List<Value> search(Resource subject, IRI predicate) {
			List<Value> values = new ArrayList<>();
			try (CloseableIteration<? extends Statement, SailCompilerException> it = connection.getStatements(subject,
					predicate, null)) {
				it.stream().forEach(s -> values.add(s.getObject()));
			}
			return values;
		}

		@Override
		public void close() throws SailCompilerException {
			connection.close();
		}
	}
}
