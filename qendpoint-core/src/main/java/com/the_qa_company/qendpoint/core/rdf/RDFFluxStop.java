package com.the_qa_company.qendpoint.core.rdf;

import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.LongFunction;

/**
 * Rdf flux stopper descriptor
 *
 * @author Antoine Willerval
 */
public abstract class RDFFluxStop {
	private static final Map<String, LongFunction<RDFFluxStop>> BUILDER = new HashMap<>();
	private static final Map<Character, BiFunction<RDFFluxStop, RDFFluxStop, RDFFluxStop>> BUILDER_OP = new HashMap<>();

	private static final RDFFluxStop EMPTY = new RDFFluxStop() {
		@Override
		public boolean canHandle(TripleString ts) {
			return false;
		}

		@Override
		public void restart() {
		}

		@Override
		public String asConfig() {
			return "";
		}
	};

	private static final RDFFluxStop NO_LIMIT = new RDFFluxStop() {
		@Override
		public boolean canHandle(TripleString ts) {
			return true;
		}

		@Override
		public void restart() {
			// nothing
		}

		@Override
		public String asConfig() {
			return HDTOptionsKeys.RDF_FLUX_STOP_VALUE_NO_LIMIT + ":0";
		}
	};

	static {
		// default rdf flux stop config
		registerCustomRDFFluxStopConfig(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_COUNT, RDFFluxStop::countLimit);
		registerCustomRDFFluxStopConfig(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE, RDFFluxStop::sizeLimit);
		registerCustomRDFFluxStopConfig(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_NO_LIMIT, l -> noLimit());

		// default rdf flux stop boolean operations
		registerCustomRDFFluxStopOperator(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_OP_AND, RDFFluxStop::and);
		registerCustomRDFFluxStopOperator(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_OP_OR, RDFFluxStop::or);
		registerCustomRDFFluxStopOperator(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_OP_NOT, (a, b) -> b.not());
	}

	/**
	 * register a custom flux stop option for the {@link #readConfig(String)}
	 * method
	 *
	 * @param name    name of the option
	 * @param builder builder
	 */
	public static void registerCustomRDFFluxStopConfig(String name, LongFunction<RDFFluxStop> builder) {
		name.chars().forEach(c -> {
			if (!Character.isJavaIdentifierPart(c)) {
				throw new IllegalArgumentException("Config can't contain non identifier part! Found '" + c + "'");
			}
		});
		BUILDER.put(name, builder);
	}

	/**
	 * register a custom flux stop operator for the {@link #readConfig(String)}
	 * method
	 *
	 * @param operator operator character
	 * @param builder  builder
	 */
	public static void registerCustomRDFFluxStopOperator(char operator,
			BiFunction<RDFFluxStop, RDFFluxStop, RDFFluxStop> builder) {
		if (Character.isJavaIdentifierPart(operator) || operator == '(' || operator == ')') {
			throw new IllegalArgumentException(
					"Operator can't be an identifier part or a parenthesis! Found '" + operator + "'");
		}
		BUILDER_OP.put(operator, builder);
	}

	private static int searchNextParenthesis(String cfg, int start) {
		int deep = 0;
		for (int i = start; i < cfg.length(); i++) {
			switch (cfg.charAt(i)) {
			case '(':
				deep++;
				break;
			case ')':
				if (deep == 0) {
					return i;
				}
				deep--;
			}
		}

		throw new IllegalArgumentException("Can't find next parenthesis for start " + start);
	}

	/**
	 * read a config to a flux stop, grammar:
	 * <p>
	 * FluxStop: limiter:number | ( FluxStop ) | Operator | (empty)
	 * </p>
	 * <p>
	 * Operator: ( FluxStop ) op ( FluxStop )
	 * </p>
	 * <p>
	 * You can register limiter with the
	 * {@link #registerCustomRDFFluxStopConfig(String, LongFunction)} method
	 * </p>
	 * <p>
	 * You can register op with the
	 * {@link #registerCustomRDFFluxStopOperator(char, BiFunction)} method
	 * </p>
	 *
	 * @param cfg   config string
	 * @param start start in the config string
	 * @param end   end in the config string
	 * @return RDFFluxStop or null if no RDFFluxStop is present
	 * @see #readConfig(String)
	 */
	public static RDFFluxStop readConfig(String cfg, int start, int end) {
		if (cfg == null) {
			return null;
		}
		int i = start;
		// current element for boolean operators
		RDFFluxStop element = null;
		while (i < end) {
			char c = cfg.charAt(i++);

			if (c == '(') { // start of block
				if (element != null) {
					throw new IllegalArgumentException(
							"Find an element after another one without having an operator! " + (i - 1));
				}
				int next = searchNextParenthesis(cfg, i);
				element = readConfig(cfg, i, next);
				i = next + 1;

			} else if (c == ')') { // end of block, should be handled here
				throw new IllegalArgumentException("Find closing parenthesis without opening! " + (i - 1));
			} else if (Character.isJavaIdentifierPart(c)) { // start of function

				// read key
				int startElement = i - 1;
				int j = i;
				while (j < end) {
					if (!Character.isJavaIdentifierPart(cfg.charAt(j))) {
						break;
					}
					j++;
				}

				if (j == end || cfg.charAt(j) != ':') { // no value for key
					throw new IllegalArgumentException("Identifier without value: " + startElement);
				}

				String key = cfg.substring(startElement, j);

				LongFunction<RDFFluxStop> builder = BUILDER.get(key);

				if (builder == null) { // key isn't a right config
					throw new IllegalArgumentException("Can't find option: " + key);
				}

				// read value

				startElement = j + 1;
				if (startElement == end || !Character.isDigit(cfg.charAt(startElement))) { // not
																							// a
																							// number
																							// value
					throw new IllegalArgumentException("Identifier without number value: " + key + ", " + startElement);
				}

				j = startElement;
				while (j < end) {
					if (!Character.isDigit(cfg.charAt(j))) {
						break;
					}
					j++;
				}
				long value = Long.parseLong(cfg.substring(startElement, j));

				element = builder.apply(value);
				i = j;
			} else {
				// read operator or throw error
				BiFunction<RDFFluxStop, RDFFluxStop, RDFFluxStop> opFunc = BUILDER_OP.get(c);

				if (opFunc == null) {
					throw new IllegalArgumentException("Unknow component: " + c + ", " + (i - 1));
				}

				if (element == null) {
					element = EMPTY;
				}
				return opFunc.apply(element, readConfig(cfg, i, end));
			}
		}

		return element;
	}

	/**
	 * read a config to a flux stop, see {@link #readConfig(String, int, int)}
	 * for grammar
	 *
	 * @param cfg config string
	 * @return RDFFluxStop or null if no RDFFluxStop is present
	 * @see #readConfig(String, int, int)
	 */
	public static RDFFluxStop readConfig(String cfg) {
		return cfg == null ? null : readConfig(cfg, 0, cfg.length());
	}

	/**
	 * @return basic implementation without any limit
	 */
	public static RDFFluxStop noLimit() {
		return NO_LIMIT;
	}

	/**
	 * implementation of flux stop stopping after a maximum triple count
	 *
	 * @param maxTriple maximum count
	 * @return FluxStop
	 */
	public static RDFFluxStop countLimit(long maxTriple) {
		if (maxTriple <= 0) {
			throw new IllegalArgumentException("Can't have a limit of 0 or a negative value!");
		}
		return new RDFFluxStop() {
			long current = 0;

			@Override
			public boolean canHandle(TripleString ts) {
				return current++ < maxTriple;
			}

			@Override
			public void restart() {
				current = 0;
			}

			@Override
			public String asConfig() {
				return HDTOptionsKeys.RDF_FLUX_STOP_VALUE_COUNT + ":" + maxTriple;
			}
		};
	}

	/**
	 * implementation of flux stop stopping after a maximum NTriple size
	 *
	 * @param maxSize maximum size
	 * @return FluxStop
	 */
	public static RDFFluxStop sizeLimit(long maxSize) {
		if (maxSize <= 0) {
			throw new IllegalArgumentException("Can't have a limit of 0 or a negative value!");
		}
		return new RDFFluxStop() {
			long size = 0;

			@Override
			public boolean canHandle(TripleString ts) {
				long tsSize;
				try {
					tsSize = ts.asNtriple().toString().getBytes(StandardCharsets.UTF_8).length;
				} catch (IOException e) {
					throw new RuntimeException("Can't estimate the size of the triple " + ts, e);
				}
				try {
					return size < maxSize;
				} finally {
					size += tsSize;
				}
			}

			@Override
			public void restart() {
				size = 0;
			}

			@Override
			public String asConfig() {
				return HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE + ":" + maxSize;
			}
		};
	}

	/**
	 * should we stop the flux after this triple or not?
	 *
	 * @param ts the triple
	 * @return true if the flux can handle this triple, false otherwise
	 */
	public abstract boolean canHandle(TripleString ts);

	/**
	 * restart the flux stop
	 */
	public abstract void restart();

	/**
	 * @return config value for the {@link HDTOptionsKeys#RDF_FLUX_STOP_KEY}
	 *         option
	 */
	public abstract String asConfig();

	@Override
	public String toString() {
		return asConfig();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof RDFFluxStop)) {
			return false;
		}
		RDFFluxStop fluxStop = (RDFFluxStop) obj;

		return asConfig().equals(fluxStop.asConfig());
	}

	/**
	 * combine 2 rdf flux stop with a boolean operation
	 *
	 * @param fluxStop     the other flux stop
	 * @param charOperator operator for the {@link #asConfig()} version
	 * @param operator     the operator
	 * @return rdffluxstop
	 * @see #and(RDFFluxStop)
	 * @see #or(RDFFluxStop)
	 */
	public RDFFluxStop booleanOp(RDFFluxStop fluxStop, char charOperator, BinaryOperator<Boolean> operator) {
		if (fluxStop == null) {
			return this;
		}
		return new RDFFluxStop() {
			@Override
			public boolean canHandle(TripleString ts) {
				boolean left = RDFFluxStop.this.canHandle(ts);
				boolean right = fluxStop.canHandle(ts);
				return operator.apply(left, right);
			}

			@Override
			public void restart() {
				RDFFluxStop.this.restart();
				fluxStop.restart();
			}

			@Override
			public String asConfig() {
				String left = RDFFluxStop.this.asConfig();
				String right = fluxStop.asConfig();
				return "(" + left + ")" + charOperator + "(" + right + ")";
			}
		};
	}

	/**
	 * {@link #booleanOp(RDFFluxStop, char, BinaryOperator)} version for AND
	 *
	 * @param fluxStop other flux stop
	 * @return rdffluxstop
	 */
	public RDFFluxStop and(RDFFluxStop fluxStop) {
		return booleanOp(fluxStop, HDTOptionsKeys.RDF_FLUX_STOP_VALUE_OP_AND, (a, b) -> a && b);
	}

	/**
	 * {@link #booleanOp(RDFFluxStop, char, BinaryOperator)} version for OR
	 *
	 * @param fluxStop other flux stop
	 * @return rdffluxstop
	 */
	public RDFFluxStop or(RDFFluxStop fluxStop) {
		return booleanOp(fluxStop, HDTOptionsKeys.RDF_FLUX_STOP_VALUE_OP_OR, (a, b) -> a || b);
	}

	/**
	 * {@link #booleanOp(RDFFluxStop, char, BinaryOperator)} version for NOT
	 *
	 * @return rdffluxstop
	 */
	public RDFFluxStop not() {
		return EMPTY.booleanOp(this, HDTOptionsKeys.RDF_FLUX_STOP_VALUE_OP_NOT, (a, b) -> !b);
	}
}
