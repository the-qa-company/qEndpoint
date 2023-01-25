package com.the_qa_company.qendpoint.compiler;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Schema describing the model sail nodes,
 * {@literal @prefix mdlc: <http://the-qa-company.com/modelcompiler/>}
 *
 * @author Antoine Willerval
 */
public class SailCompilerSchema {
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final Comparator<IRI> IRI_COMPARATOR = (iri1, iri2) -> iri1.toString()
			.compareToIgnoreCase(iri2.toString());
	private static final Map<IRI, Property<?, ?>> DESC = new TreeMap<>(IRI_COMPARATOR);
	/**
	 * {@literal @prefix mdlc: <http://the-qa-company.com/modelcompiler/>}
	 */
	public static final String COMPILER_NAMESPACE = "http://the-qa-company.com/modelcompiler/";
	/**
	 * prefix used in the description
	 */
	public static final String PREFIX = "mdlc:";
	/**
	 * mdlc:main
	 */
	public static final IRI MAIN = iri("main", "The main node, the start point of the model");
	/**
	 * mdlc:node
	 */
	public static final IRI NODE = iri("node", "Describe a node");
	/**
	 * mdlc:filter
	 */
	public static final IRI TYPE = iri("type", "Describe the type of a node");
	/**
	 * mdlc:paramKey
	 */
	public static final IRI PARAM_KEY = iri("paramKey", "Describe a parameter key");
	/**
	 * mdlc:paramValue
	 */
	public static final IRI PARAM_VALUE = iri("paramValue", "Describe a parameter value");
	/**
	 * mdlc:dirParam
	 */
	public static final IRI PARSED_STRING_PARAM = iri("parsedStringParam", "Describe a parsed string param");
	/**
	 * mdlc:parsedString
	 */
	public static final IRI PARSED_STRING_DATATYPE = iri("parsedString", "Describe a parsed string datatype");
	/**
	 * mdlc:paramLink
	 */
	public static final IRI PARAM_LINK = iri("paramLink", "Describe a node param linked sails");
	/**
	 * mdlc:paramFilter
	 */
	public static final IRI PARAM_FILTER = iri("paramFilter", "Describe a node param filter");
	/**
	 * mdlc:paramFilterAnd
	 */
	public static final IRI PARAM_FILTER_AND = iri("paramFilterAnd", "Describe a node param filter op AND");
	/**
	 * mdlc:paramFilterOr
	 */
	public static final IRI PARAM_FILTER_OR = iri("paramFilterOr", "Describe a node param filter op OR");
	/**
	 * mdlc:predicateFilter
	 */
	public static final IRI PARAM_FILTER_TYPE_PREDICATE = iri("predicateFilter", "Describe the type predicate Filter");
	/**
	 * mdlc:languageFilter
	 */
	public static final IRI PARAM_FILTER_TYPE_LANGUAGE = iri("languageFilter", "Describe the type language Filter");
	/**
	 * mdlc:languageFilterLang
	 */
	public static final IRI PARAM_FILTER_TYPE_LANGUAGE_LANG = iri("languageFilterLang",
			"Describe the type language Filter param");
	/**
	 * mdlc:acceptNoLanguageLiterals
	 */
	public static final IRI PARAM_FILTER_TYPE_LANGUAGE_NO_LANG_LIT = iri("acceptNoLanguageLiterals",
			"Describe the type language Filter param");
	/**
	 * mdlc:typeFilter
	 */
	public static final IRI PARAM_FILTER_TYPE_TYPE = iri("typeFilter", "Describe the predicate type type");
	/**
	 * mdlc:typeFilter
	 */
	public static final IRI PARAM_FILTER_TYPE_TYPE_PREDICATE = iri("typeFilterPredicate",
			"Describe the predicate type type param type predicate");
	/**
	 * mdlc:typeFilterObject
	 */
	public static final IRI PARAM_FILTER_TYPE_TYPE_OBJECT = iri("typeFilterObject",
			"Describe the predicate type type param object");
	/**
	 * mdlc:typeFilterLuceneExp
	 */
	public static final IRI PARAM_FILTER_TYPE_LUCENE_EXP = iri("typeFilterLuceneExp",
			"Describe the lucene exp predicate type");
	/**
	 * mdlc:typeFilterLuceneGeoExp
	 */
	public static final IRI PARAM_FILTER_TYPE_LUCENE_GEO_EXP = iri("typeFilterLuceneGeoExp",
			"Describe the lucene geo exp predicate type");
	/**
	 * mdlc:filterNode
	 */
	public static final IRI FILTER_TYPE = iri("filterNode", "Describe the type Filter");
	/**
	 * mdlc:multiFilterNode
	 */
	public static final IRI MULTI_FILTER_TYPE = iri("multiFilterNode", "Describe the type Multi-Filter");
	/**
	 * mdlc:linkedSailNode
	 */
	public static final IRI LINKED_SAIL_TYPE = iri("linkedSailNode", "Describe the type Linked sail");
	/**
	 * mdlc:luceneNode
	 */
	public static final IRI LUCENE_TYPE = iri("luceneNode", "Describe the type Lucene");
	/**
	 * mdlc:luceneLang
	 */
	public static final IRI LUCENE_TYPE_LANG = iri("luceneLang", "Describe the Lucene language(s)");
	/**
	 * mdlc:luceneEvalMode
	 */
	public static final IRI LUCENE_TYPE_EVAL_MODE = iri("luceneEvalMode", "Describe the Lucene evaluation mode");
	/**
	 * mdlc:genHDTOption
	 */
	public static final IRI GEN_HDT_OPTION_PARAM = iri("genHDTOption", "Describe the HDT options");
	/**
	 * mdlc:luceneParam
	 */
	public static final IRI LUCENE_TYPE_PARAM = iri("luceneParam", "Describe a Lucene param");
	/**
	 * mdlc:luceneReindexQuery
	 */
	public static final IRI LUCENE_TYPE_REINDEX_QUERY = iri("luceneReindexQuery", "Describe the Lucene reindex query");
	/**
	 * mdlc:dirLocation
	 */
	public static final IRI DIR_LOCATION = iri("dirLocation", "Describe a directory");
	/**
	 * mdlc:storageMode
	 */
	public static final Property<IRI, IRITypeValueHandler> STORAGE_MODE_PROPERTY = propertyIri("storageMode",
			"The storage mode");
	/**
	 * mdlc:storageMode
	 */
	public static final IRI STORAGE_MODE = STORAGE_MODE_PROPERTY.getIri();
	/**
	 * mdlc:endpointStoreStorage
	 */
	public static final IRI ENDPOINTSTORE_STORAGE = STORAGE_MODE_PROPERTY.getHandler()
			.createValue("endpointStoreStorage", "The storage mode endpoint store", true);
	/**
	 * mdlc:nativeStoreStorage
	 */
	public static final IRI NATIVESTORE_STORAGE = STORAGE_MODE_PROPERTY.getHandler().createValue("nativeStoreStorage",
			"The storage mode native store");
	/**
	 * mdlc:memoryStoreStorage
	 */
	public static final IRI MEMORYSTORE_STORAGE = STORAGE_MODE_PROPERTY.getHandler().createValue("memoryStoreStorage",
			"The storage mode memory store");
	/**
	 * mdlc:lmdbStoreStorage
	 */
	public static final IRI LMDB_STORAGE = STORAGE_MODE_PROPERTY.getHandler().createValue("lmdbStoreStorage",
			"The storage mode lmdb");

	/**
	 * mdlc:hdtSpec
	 */
	public static final Property<String, StringTypeValueHandler> HDT_SPEC_PROPERTY = propertyStr("hdtSpec",
			"The hdt spec", "");

	/**
	 * mdlc:hdtReadMode PROPERTY
	 */
	public static final Property<IRI, IRITypeValueHandler> HDT_READ_MODE_PROPERTY = propertyIri("hdtReadMode",
			"The hdt reading mode");
	/**
	 * mdlc:hdtReadMode PROPERTY
	 */
	public static final IRI HDT_READ_MODE = HDT_READ_MODE_PROPERTY.getIri();
	/**
	 * mdlc:hdtLoadReadMode
	 */
	public static final IRI HDT_READ_MODE_LOAD = HDT_READ_MODE_PROPERTY.getHandler().createValue("hdtLoadReadMode",
			"The hdt load reading mode, load the full HDT into memory");
	/**
	 * mdlc:hdtMapReadMode
	 */
	public static final IRI HDT_READ_MODE_MAP = HDT_READ_MODE_PROPERTY.getHandler().createValue("hdtMapReadMode",
			"The hdt load reading mode, map the HDT into memory", true);

	/**
	 * mdlc:rdfStoreSplit
	 */
	public static final Property<Integer, NumberTypeValueHandler> RDF_STORE_SPLIT_STORAGE = propertyInt("rdfStoreSplit",
			"The storage load split update count", 1000, 1);

	/**
	 * mdlc:endpointThreshold
	 */
	public static final Property<Integer, NumberTypeValueHandler> ENDPOINT_THRESHOLD = propertyInt("endpointThreshold",
			"The threshold before merging the endpoint", 1_000_000, 0);
	/**
	 * mdlc:serverPort
	 */
	public static final Property<Integer, NumberTypeValueHandler> SERVER_PORT = propertyInt("serverPort",
			"The endpoint server port", 1234, 1, (2 << 15) - 1);
	/**
	 * mdlc:timeoutUpdate
	 */
	public static final Property<Integer, NumberTypeValueHandler> TIMEOUT_UPDATE = propertyInt("timeoutUpdate",
			"the maximum time for update query, in second", 300, 0);
	/**
	 * mdlc:timeoutQuery
	 */
	public static final Property<Integer, NumberTypeValueHandler> TIMEOUT_QUERY = propertyInt("timeoutQuery",
			"the maximum time for non-update query, in second", 300, 0);

	/**
	 * mdlc:hdtPassMode property
	 */
	public static final Property<IRI, IRITypeValueHandler> HDT_PASS_MODE_PROPERTY = propertyIri("hdtPassMode",
			"The mode to parse the Triple flux");
	/**
	 * mdlc:hdtPassMode
	 */
	public static final IRI HDT_PASS_MODE = HDT_PASS_MODE_PROPERTY.getIri();
	/**
	 * mdlc:hdtOnePassMode
	 */
	public static final IRI HDT_ONE_PASS_MODE = HDT_PASS_MODE_PROPERTY.getHandler().createValue("hdtOnePassMode",
			"The mode to parse the Triple flux in one pass, reduce disk usage");
	/**
	 * mdlc:hdtTwoPassMode
	 */
	public static final IRI HDT_TWO_PASS_MODE = HDT_PASS_MODE_PROPERTY.getHandler().createValue("hdtTwoPassMode",
			"The mode to parse the Triple flux in two passes, reduce time usage", true);

	/**
	 * mdlc:option property
	 */
	public static final Property<IRI, IRITypeValueHandler> OPTION_PROPERTY = propertyIri("option", "option predicate");
	/**
	 * mdlc:option
	 */
	public static final IRI OPTION = OPTION_PROPERTY.getIri();
	/**
	 * mdlc:debugShowTime
	 */
	public static final IRI DEBUG_SHOW_TIME = OPTION_PROPERTY.getHandler().createValue("debugShowTime",
			"Show exec time of query");
	/**
	 * mdlc:debugShowPlan
	 */
	public static final IRI DEBUG_SHOW_PLAN = OPTION_PROPERTY.getHandler().createValue("debugShowPlan",
			"Show query plans");
	/**
	 * mdlc:debugDisableOptionReloading
	 */
	public static final IRI DEBUG_DISABLE_OPTION_RELOADING = OPTION_PROPERTY.getHandler()
			.createValue("debugDisableOptionReloading", "Disable option reloading");
	/**
	 * mdlc:debugShowQueryResultCount
	 */
	public static final IRI DEBUG_SHOW_QUERY_RESULT_COUNT = OPTION_PROPERTY.getHandler()
			.createValue("debugShowQueryResultCount", "Show query count");
	/**
	 * mdlc:noOptimization
	 */
	public static final IRI NO_OPTIMIZATION = OPTION_PROPERTY.getHandler().createValue("noOptimization",
			"Disable optimization for native stores");

	private static IRI iri(String name, String desc) {
		return propertyVoid(name, desc).getIri();
	}

	private static Property<IRI, IRITypeValueHandler> propertyIri(String name, String desc) {
		return property(name, desc, new IRITypeValueHandler());
	}

	private static Property<String, StringTypeValueHandler> propertyStr(String name, String desc, String defaultValue) {
		return property(name, desc, new StringTypeValueHandler(defaultValue));
	}

	private static Property<Integer, NumberTypeValueHandler> propertyInt(String name, String desc, int defaultValue) {
		return property(name, desc, new NumberTypeValueHandler(defaultValue));
	}

	private static Property<Integer, NumberTypeValueHandler> propertyInt(String name, String desc, int defaultValue,
			int min) {
		return property(name, desc, new NumberTypeValueHandler(defaultValue).withMin(min));
	}

	private static Property<Integer, NumberTypeValueHandler> propertyInt(String name, String desc, int defaultValue,
			int min, int max) {
		return property(name, desc, new NumberTypeValueHandler(defaultValue).withRange(min, max));
	}

	private static Property<Value, ValueHandler<Value>> propertyVoid(String name, String desc) {
		return property(name, desc, ValueHandler.id());
	}

	private static <T, H extends ValueHandler<T>> Property<T, H> property(String name, String desc, H handler) {
		IRI iri = VF.createIRI(COMPILER_NAMESPACE + name);
		Property<T, H> prop = new Property<>(iri, desc, PREFIX + name, handler);
		Property<?, ?> old = DESC.put(iri, prop);
		assert old == null : "Iri already registered: " + iri;
		return prop;
	}

	private static String mdEscapeLink(String title) {
		return title.toLowerCase(Locale.ROOT).replaceAll("[:]", "");
	}

	/**
	 * convert the descriptions in {@link #getDescriptions()} into Markdown and
	 * write it into a stream
	 *
	 * @param stream stream to write the markdown
	 */
	public static void writeToMarkdown(OutputStream stream) {
		PrintWriter w = new PrintWriter(stream, true);

		// write header and table of content
		w.println("# Sail sail schema");
		w.println();
		w.println("```turtle");
		w.println("@prefix " + PREFIX + " <" + COMPILER_NAMESPACE + ">");
		w.println("```");
		w.println();

		for (Property<?, ?> property : getDescriptions().values()) {
			w.println("- [``" + property.getTitle() + "``](#" + mdEscapeLink(property.getTitle()) + ")");
		}
		w.println();

		// write body
		for (Property<?, ?> property : getDescriptions().values()) {
			w.println("## `" + property.getTitle() + "`");
			w.println();
			w.println("**IRI**: [" + property.getIri() + "](" + property.getIri() + ")");
			w.println();
			w.println("### Description");
			w.println();
			w.println(property.getDescription());
			w.println();

			if (property.getHandler() instanceof IRITypeValueHandler) {
				Set<IRI> values = ((IRITypeValueHandler) property.getHandler()).getValues();
				if (!values.isEmpty()) {
					w.println("### Values");
					w.println();
					IRI defaultValue = ((IRITypeValueHandler) property.getHandler()).defaultValue;
					if (defaultValue != null) {
						Property<?, ?> defaultValueProp = getDescriptions().getOrDefault(defaultValue, null);
						if (defaultValueProp == null) {
							w.println("Default value: [" + defaultValue + "](" + defaultValue + ")");
						} else {
							w.println("Default value: [" + defaultValueProp.getTitle() + "](#"
									+ mdEscapeLink(defaultValueProp.getTitle()) + ")");
						}
						w.println();
					}
					w.println("Usable value(s) for this property:");
					w.println();
					for (IRI value : values) {
						Property<?, ?> valueProp = getDescriptions().getOrDefault(value, null);
						if (valueProp == null) {
							w.println("- [" + value + "](" + value + ")");
						} else {
							w.println("- [" + valueProp.getTitle() + "](#" + mdEscapeLink(valueProp.getTitle()) + ")");
						}
					}
					w.println();
				}
			} else if (property.getHandler() instanceof NumberTypeValueHandler) {
				w.println("### Value");
				w.println();
				w.println("Number value");
				w.println();
				NumberTypeValueHandler h = (NumberTypeValueHandler) property.getHandler();

				w.println("- default value: " + h.defaultValue());

				if (h.min != Integer.MIN_VALUE) {
					w.println("- min value: " + h.getMin());
				}
				if (h.max != Integer.MAX_VALUE) {
					w.println("- max value: " + h.getMax());
				}
				w.println();
			}
			w.println("---");
			w.println("");
		}
	}

	public static void main(String[] args) throws IOException {
		try (FileOutputStream out = new FileOutputStream("COMPILER_SCHEMA.MD")) {
			writeToMarkdown(out);
		}
	}

	/**
	 * @return a description map indexed by IRI
	 */
	public static Map<IRI, Property<?, ?>> getDescriptions() {
		return Collections.unmodifiableMap(DESC);
	}

	private SailCompilerSchema() {
	}

	/**
	 * A property predicate in the schema
	 */
	public static class Property<V, T extends ValueHandler<V>> {
		private final IRI iri;
		private final String title;
		private final String description;
		private final T handler;

		private Property(IRI iri, String description, String title, T handler) {
			this.iri = iri;
			this.title = title;
			this.description = description;
			this.handler = handler;
			handler.setParent(this);
		}

		public String getTitle() {
			return title;
		}

		public T getHandler() {
			return handler;
		}

		/**
		 * @return the associate predicate iri
		 */
		public IRI getIri() {
			return iri;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return description;
		}

		/**
		 * throw if an value isn't in the valid values
		 *
		 * @param value the value to test
		 * @return the value
		 * @throws SailCompiler.SailCompilerException if the value isn't valid
		 */
		public V throwIfNotValidValue(Value value) throws SailCompiler.SailCompilerException {
			return handler.validate(value);
		}
	}

	/**
	 * value handler for a property
	 *
	 * @param <T> the output value
	 */
	public interface ValueHandler<T> {
		static ValueHandler<Value> id() {
			return v -> v;
		}

		/**
		 * validate and return the converted value
		 *
		 * @param v the value to check
		 * @return the converted value
		 * @throws SailCompiler.SailCompilerException if the value isn't valid
		 */
		T validate(Value v) throws SailCompiler.SailCompilerException;

		default T defaultValue() {
			throw new SailCompiler.SailCompilerException("No default value for this property");
		}

		default void setParent(Property<T, ? extends ValueHandler<T>> p) {
		}
	}

	public static class StringTypeValueHandler implements ValueHandler<String> {
		private Property<String, ? extends ValueHandler<String>> parent;
		private final String defaultValue;
		private final Pattern regex;

		public StringTypeValueHandler(String defaultValue) {
			this(defaultValue, (Pattern) null);
		}

		public StringTypeValueHandler(String defaultValue, String regex) {
			this(defaultValue, regex == null ? null : Pattern.compile(regex));
		}

		public StringTypeValueHandler(String defaultValue, Pattern regex) {
			this.defaultValue = defaultValue;
			this.regex = regex;
		}

		@Override
		public void setParent(Property<String, ? extends ValueHandler<String>> p) {
			assert parent == null : "parent can't be null";
			parent = p;
		}

		@Override
		public String defaultValue() {
			return defaultValue;
		}

		@Override
		public String validate(Value v) throws SailCompiler.SailCompilerException {
			if (!(v instanceof Literal)) {
				throw new SailCompiler.SailCompilerException(
						v + " is not a valid literal value for the property " + parent);
			}
			String val = v.stringValue();
			if (regex != null) {
				if (!regex.matcher(val).matches()) {
					throw new SailCompiler.SailCompilerException(
							v + " is not a valid value for the property " + parent + ", regex: " + regex);
				}
			}
			return val;
		}
	}

	public static class IRITypeValueHandler implements ValueHandler<IRI> {
		private Property<IRI, ? extends ValueHandler<IRI>> parent;
		private final Set<IRI> values;
		private IRI defaultValue;

		public IRITypeValueHandler() {
			this.values = new TreeSet<>(IRI_COMPARATOR);
		}

		@Override
		public void setParent(Property<IRI, ? extends ValueHandler<IRI>> p) {
			assert parent == null : "parent can't be null";
			parent = p;
		}

		private IRI createValue(IRI value) {
			return createValue(value, false);
		}

		private IRI createValue(String name, String description) {
			return createValue(iri(name, description));
		}

		private IRI createValue(IRI value, boolean defaultValue) {
			values.add(value);
			if (defaultValue) {
				if (this.defaultValue != null) {
					throw new IllegalArgumentException("default value already defined");
				}
				this.defaultValue = value;
			}
			return value;
		}

		@Override
		public IRI defaultValue() {
			return defaultValue;
		}

		private IRI createValue(String name, String description, boolean defaultValue) {
			return createValue(iri(name, description), defaultValue);
		}

		/**
		 * @return the valid values for this predicate
		 */
		public Set<IRI> getValues() {
			return Collections.unmodifiableSet(values);
		}

		@Override
		public IRI validate(Value v) throws SailCompiler.SailCompilerException {
			if (!(v instanceof IRI)) {
				throw new SailCompiler.SailCompilerException(
						v + " is not a valid iri value for the property " + parent);
			}
			if (values.contains(v)) {
				return (IRI) v;
			}
			throw new SailCompiler.SailCompilerException(v + " is not a valid value for the property " + parent);
		}

	}

	public static class NumberTypeValueHandler implements ValueHandler<Integer> {
		private Property<Integer, ? extends ValueHandler<Integer>> parent;
		private int min = Integer.MIN_VALUE;
		private int max = Integer.MAX_VALUE;
		private final int defaultValue;

		public NumberTypeValueHandler(int defaultValue) {
			this.defaultValue = defaultValue;
		}

		public int getMax() {
			return max;
		}

		public int getMin() {
			return min;
		}

		@Override
		public void setParent(Property<Integer, ? extends ValueHandler<Integer>> p) {
			if (parent != null) {
				throw new IllegalArgumentException("parent not null");
			}
			parent = p;
		}

		public NumberTypeValueHandler withMax(int max) {
			this.max = max;
			return this;
		}

		public NumberTypeValueHandler withMin(int min) {
			this.min = min;
			return this;
		}

		public NumberTypeValueHandler withRange(int min, int max) {
			return withMin(min).withMax(max);
		}

		@Override
		public Integer validate(Value v) throws SailCompiler.SailCompilerException {
			if (!(v instanceof Literal)) {
				throw new SailCompiler.SailCompilerException(
						v + " is not a valid literal value for the property " + parent);
			}
			Literal l = (Literal) v;
			if (!l.getCoreDatatype().asXSDDatatype().orElseThrow(() -> new SailCompiler.SailCompilerException(
					l + " is not a valid number xsd literal for the property " + parent)).isIntegerDatatype()) {
				throw new SailCompiler.SailCompilerException(
						l + " is not a valid number literal for the property " + parent);
			}
			int value = ((Literal) v).intValue();

			if (value > max || value < min) {
				throw new SailCompiler.SailCompilerException(
						l + " out of range(" + min + ", " + max + ") for the property " + parent);
			}
			return value;
		}

		@Override
		public Integer defaultValue() {
			return defaultValue;
		}
	}
}
