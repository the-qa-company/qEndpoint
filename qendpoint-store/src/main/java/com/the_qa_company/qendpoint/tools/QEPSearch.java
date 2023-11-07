package com.the_qa_company.qendpoint.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleBaseDictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.enums.WikidataChangesFlavor;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.parsers.RDFDeltaFileParser;
import com.the_qa_company.qendpoint.core.tools.HDTVerify;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.Profiler;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.UnicodeEscape;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;
import com.the_qa_company.qendpoint.model.SimpleBNodeHDT;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import com.the_qa_company.qendpoint.model.SimpleLiteralHDT;
import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.HDTConverter;
import com.the_qa_company.qendpoint.utils.FormatUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author mario.arias
 */
public class QEPSearch {
	@Parameter(description = "<File>")
	public List<String> parameters = new ArrayList<>();

	@Parameter(names = "-options", description = "HDT Conversion options (override those of config file)")
	public String options;

	@Parameter(names = "-config", description = "Conversion config file")
	public String configFile;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-type", description = "partition type, 'delta', 'hdt', 'qendpoint', 'sequence', 'reader' or 'guess' (default)")
	public String type;

	@Parameter(names = "-rdf4jfixdump", description = "fix RDF4J NativeStore triple issues")
	public boolean rdf4jfixdump;

	@Parameter(names = "-rdf4jindex", description = "RDF4J indexes, default spoc,posc,cosp")
	public String rdf4jindex;

	@Parameter(names = "-version", description = "Prints the HDT version number")
	public static boolean showVersion;

	@Parameter(names = "-searchCfg", description = "HDT Conversion options (override those of config file)")
	public String searchCfg;
	@Parameter(names = "-binindex", description = "Prints bin index if implemented")
	public boolean showBinIndex;
	@Parameter(names = "-nocrc", description = "Avoid CRC checks")
	public boolean noCRC;

	public String input;

	private ColorTool colorTool;

	private final HDTOptions configData = HDTOptions.of();

	@Parameter(names = "-memory", description = "Load the whole file into main memory. Ensures fastest querying.")
	public boolean loadInMemory;

	protected void iterate(HDT hdt, CharSequence subject, CharSequence predicate, CharSequence object)
			throws NotFoundException {
		StopWatch iterateTime = new StopWatch();
		int count;

		System.out.println("Query " + subject + " " + predicate + " " + object + ":");

		subject = subject.length() == 1 && subject.charAt(0) == '?' ? "" : subject;
		predicate = predicate.length() == 1 && predicate.charAt(0) == '?' ? "" : predicate;
		object = object.length() == 1 && object.charAt(0) == '?' ? "" : object;

		boolean showIndex = showIndex();

		if (predicate.isEmpty() && subject.isEmpty() && object.isEmpty() && noSPO()) {
			System.err.println(colorTool.red() + "Can't do SPO with NO_SPO=true");
			return;
		}
		// Iterate over triples as Strings
		IteratorTripleString it = hdt.search(subject, predicate, object);
		count = 0;
		while (it.hasNext()) {
			TripleString triple = it.next();
			System.out.println(
					pretty(triple) + (showIndex ? colorTool.yellow() + " (" + it.getLastTriplePosition() + ")" : "")
							+ colorTool.colorReset());
			count++;
		}

//		Iterate over triples only as IDs
//		TripleID patternID = DictionaryUtil.tripleStringtoTripleID(hdt.getDictionary(), new TripleString(subject, predicate, object));
//		IteratorTripleID it2 = hdt.getTriples().search(patternID);
//		while(it2.hasNext()) {
//			TripleID triple = it2.next();
//			System.out.println(triple);
//			count++;
//		}
		System.out.println(colorTool.cyan() + "Iterated " + count + " triples in " + iterateTime.stopAndShow());
	}

	protected void iterate(NotifyingSail store, CharSequence subject, CharSequence predicate, CharSequence object,
			Function<Value, Value> valueConverter) throws NotFoundException {
		StopWatch iterateTime = new StopWatch();
		int count;

		subject = subject.length() == 1 && subject.charAt(0) == '?' ? "" : subject;
		predicate = predicate.length() == 1 && predicate.charAt(0) == '?' ? "" : predicate;
		object = object.length() == 1 && object.charAt(0) == '?' ? "" : object;

		// Iterate over triples as Strings
		try (NotifyingSailConnection conn = store.getConnection()) {
			final ValueFactory vf = store.getValueFactory();
			Resource res;
			IRI pred;
			Value obj;

			if (subject.isEmpty()) {
				res = null;
			} else if (subject.charAt(0) == '_') {
				res = vf.createBNode(subject.toString().substring(2));
			} else if (subject.charAt(0) == '<') {
				res = vf.createIRI(subject.toString().substring(1, subject.length() - 1));
			} else {
				res = vf.createIRI(subject.toString());
			}
			if (predicate.isEmpty()) {
				pred = null;
			} else if (predicate.charAt(0) == '<') {
				pred = vf.createIRI(predicate.toString().substring(1, predicate.length() - 1));
			} else {
				pred = vf.createIRI(predicate.toString());
			}
			if (object.isEmpty()) {
				obj = null;
			} else if (object.charAt(0) == '"') {
				String type = LiteralsUtils.getType(object).toString();
				switch (type) {
				case LiteralsUtils.NO_DATATYPE_STR -> obj = vf.createLiteral(object.toString());
				case LiteralsUtils.LITERAL_LANG_TYPE_STR -> {
					String value = LiteralsUtils.removeLang(object).toString();
					String lang = LiteralsUtils.getLanguage(object).map(CharSequence::toString).orElse(null);
					obj = vf.createLiteral(value.substring(1, value.length() - 1), lang);
				}
				default -> {
					String value = LiteralsUtils.removeType(object).toString();
					obj = vf.createLiteral(value.substring(1, value.length() - 1),
							vf.createIRI(type.substring(1, type.length() - 1)));
				}
				}
			} else if (object.charAt(0) == '_') {
				obj = vf.createBNode(object.toString().substring(2));
			} else if (object.charAt(0) == '<') {
				obj = vf.createIRI(object.toString().substring(1, object.length() - 1));
			} else {
				obj = vf.createIRI(object.toString());
			}
			Resource ms = (Resource) valueConverter.apply(res);
			if (ms != null) {
				res = ms;
			}
			IRI mp = (IRI) valueConverter.apply(pred);
			if (mp != null) {
				pred = mp;
			}
			Value mo = valueConverter.apply(obj);
			if (mo != null) {
				obj = mo;
			}
			System.out.println("Query " + prettyComponent(res) + " " + prettyComponent(pred) + " "
					+ prettyComponent(obj) + colorTool.colorReset());
			if (ms == null && mp == null && mo == null && noSPO()) {
				System.err.println(colorTool.red() + "Can't do SPO with NO_SPO=true" + colorTool.colorReset());
				return;
			}
			try (CloseableIteration<? extends Statement> it = conn.getStatements(ms, mp, mo, false)) {
				count = 0;
				while (it.hasNext()) {
					Statement triple = it.next();
					System.out.println(pretty(triple));
					count++;
				}

				System.out.println(colorTool.cyan() + "Iterated " + count + " triples in " + iterateTime.stopAndShow()
						+ colorTool.colorReset());
			}
		}
	}

	enum Type {
		DELTA, HDT,
		QENDPOINT("Use 'bit[dxyz] <id>' to print an index in a bitmap", "Use 'integrity <id>' to check QEP integrity",
				"'hdth' print HDT header", "'da <query>' Query delta store A", "'db <query>' Query delta store B",
				"'hdt <query>' Query main store", "'store' says which store is used",
				"'tid <query>' print QEP values for a triple"),
		NONE;

		private final String[] cmd;

		Type(String... cmd) {
			this.cmd = cmd;
		}

		public String[] getCmd() {
			return cmd;
		}
	}

	private void help(Type type) {
		for (String s : new String[] { "HELP:", "Please write Triple Search Pattern, using '?' for wildcards. e.g ",
				"   http://www.somewhere.com/mysubject ? ?", "Use 'exit' or 'quit' to terminate interactive shell.",
				"Use 'config <option>=<value>' to set a config value", " config: SHOW_INDEX=true|false",
				" config: NO_SPO=true|false" }) {
			System.out.println(colorTool.cyan() + s + colorTool.colorReset());
		}
		for (String s : type.cmd) {
			System.out.println(colorTool.cyan() + s + colorTool.colorReset());
		}
	}

	private boolean showIndex() {
		return configData.getBoolean("SHOW_INDEX", false);
	}

	private boolean noSPO() {
		return configData.getBoolean("NO_SPO", false);
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line  line to parse
	 * @param start start char
	 */
	private void parseTriplePattern(TripleString dest, String line, int start) throws ParserException {
		parseTriplePattern(dest, line.substring(start));
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line line to parse
	 */
	private void parseTriplePattern(TripleString dest, String line) throws ParserException {
		int split, posa, posb;
		dest.clear();

		// SET SUBJECT
		posa = 0;
		posb = split = line.indexOf(' ', posa);

		if (posb == -1)
			throw new ParserException("Make sure that you included three terms."); // Not
		// found,
		// error.

		dest.setSubject(UnicodeEscape.unescapeString(line.substring(posa, posb)));

		// SET PREDICATE
		posa = split + 1;
		posb = split = line.indexOf(' ', posa);

		if (posb == -1)
			throw new ParserException("Make sure that you included three terms.");

		dest.setPredicate(UnicodeEscape.unescapeString(line.substring(posa, posb)));

		// SET OBJECT
		posa = split + 1;
		posb = line.length();

		if (line.charAt(posb - 1) == '.')
			posb--; // Remove trailing <space> <dot> from NTRIPLES.
		if (line.charAt(posb - 1) == ' ')
			posb--;

		dest.setObject(UnicodeEscape.unescapeString(line.substring(posa, posb)));
	}

	public void executeHDT() throws IOException {
		HDTOptions spec;
		if (configFile != null) {
			spec = HDTOptions.readFromFile(configFile);
		} else {
			spec = HDTOptions.of();
		}
		if (options != null) {
			spec.setOptions(options);
		}
		if (showBinIndex) {
			spec.set(HDTOptionsKeys.DUMP_BINARY_OFFSETS, true);
		}

		HDT hdt;
		MultiThreadListenerConsole console = colorTool.getConsole();
		if (loadInMemory) {
			hdt = HDTManager.loadIndexedHDT(input, console, spec);
		} else {
			hdt = HDTManager.mapIndexedHDT(input, spec, console);
		}

		console.unregisterAllThreads();
		console.printLine(colorTool.cyan() + "type 'help' for help, 'exit' to leave" + colorTool.colorReset());

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, UTF_8));
		try {
			TripleString triplePattern = new TripleString();

			while (true) {
				System.out.print(colorTool.white() + ">> " + colorTool.white());
				System.out.flush();
				String line = in.readLine();
				System.out.print(colorTool.colorReset());
				if (line == null || line.equals("exit") || line.equals("quit")) {
					break;
				}
				if (executeSub(line, Type.HDT)) {
					continue;
				}

				try {
					parseTriplePattern(triplePattern, line);
					iterate(hdt, triplePattern.getSubject(), triplePattern.getPredicate(), triplePattern.getObject());
				} catch (ParserException e) {
					System.err.println(colorTool.red() + "Could not parse triple pattern: " + e.getMessage()
							+ colorTool.colorReset());
					help(Type.HDT);
				} catch (NotFoundException e) {
					System.err.println(colorTool.red() + "No results found." + colorTool.colorReset());
				}
			}
		} finally {
			if (hdt != null)
				hdt.close();
			in.close();
		}
	}

	private void dumpRDF4JNativeStore(File store) {

	}

	public void executeDelta() throws IOException {
		if (rdf4jfixdump) {
			dumpRDF4JNativeStore(new File(input));
			return;
		}

		NativeStore store = new NativeStore(new File(input), Objects.requireNonNullElse(rdf4jindex, "spoc,posc,cosp"));

		try {
			executeSail(store, Function.identity(), Type.DELTA);
		} finally {
			store.shutDown();
		}
	}

	public void executeDeltaQEndpoint() throws IOException {
		HDTOptions spec;
		if (configFile != null) {
			spec = HDTOptions.readFromFile(configFile);
		} else {
			spec = HDTOptions.of();
		}
		if (options != null) {
			spec.setOptions(options);
		}

		EndpointStore store = new EndpointStore(Path.of(input), spec);

		try {
			executeSail(store, store.getHdtConverter()::convertValue, Type.QENDPOINT);
		} finally {
			store.shutDown();
		}
	}

	public boolean executeSub(String line, Type type) {
		if (line.equals("help")) {
			help(type);
			return true;
		}

		if (line.startsWith("config")) {
			if (line.length() == "config".length()) {
				System.out.println(colorTool.red() + "config <opt>=<value>" + colorTool.colorReset());
				help(type);
			} else {
				try {
					configData.setOptions(line.substring("config ".length()));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return true;
		}

		return false;
	}

	private void integrityTest(EndpointStore ep) {
		colorTool.log("Running Delta integrity checks");
		long errors = 0;
		Sail sail = ep.getChangingStore();
		MultiThreadListenerConsole console = colorTool.getConsole();
		console.notifyProgress(0, "open connection to native store");
		try (SailConnection conn = sail.getConnection()) {
			try (CloseableIteration<? extends Statement> it = conn.getStatements(null, null, null, false)) {
				long triples = 0;
				HDTConverter converter = ep.getHdtConverter();
				long shared = ep.getHdt().getDictionary().getNshared();
				while (it.hasNext()) {
					Statement stmt = it.next();
					long s = converter.rdf4jSubjectToHdtID(stmt.getSubject());
					long p = converter.rdf4jPredicateToHdtID(stmt.getPredicate());
					long o = converter.rdf4jObjectToHdtID(stmt.getObject());

					boolean missingS;
					boolean missingP;
					boolean missingO;

					if (s > 0) {
						missingS = !ep.getBitX().access(s - 1);
					} else {
						missingS = false;
					}
					if (p > 0) {
						missingP = !ep.getBitY().access(p - 1);
					} else {
						missingP = false;
					}
					if (o > 0) {
						if (o <= shared) {
							missingO = !ep.getBitX().access(o - 1);
						} else {
							missingO = !ep.getBitZ().access(o - shared - 1);
						}
					} else {
						missingO = false;
					}

					if (missingS || missingP || missingO) {
						colorTool.error("Missing", "components: " + (missingS ? " missingS:" + s : "")
								+ (missingP ? " missingP:" + p : "") + (missingO ? " missingO:" + o : ""));
						errors++;
					}

					console.notifyProgress(0,
							"checked " + triples++ + " triples (" + errors + " error" + (errors > 1 ? "s" : "") + ")");
				}
				console.notifyProgress(100,
						"checked " + triples + " triples (" + errors + " error" + (errors > 1 ? "s" : "") + ")");
			}
		}
		if (errors != 0) {
			colorTool.error("Delta store not valid", true);
		} else {
			colorTool.log(colorTool.color(0, 5, 0) + "Delta store valid", true);
		}
	}

	private void integrityTest(HDT hdt) {
		colorTool.log("Running HDT integrity checks");
		MultiThreadListenerConsole console = colorTool.getConsole();

		boolean binary = configData.getBoolean("integrity.binary", false);
		boolean unicode = configData.getBoolean("integrity.unicode", true);

		boolean error;
		long count = 0;
		if (hdt.getDictionary() instanceof MultipleBaseDictionary) {
			colorTool.log("Checking subject entries");
			error = HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "subject",
					hdt.getDictionary().getSubjects(), console);
			count += hdt.getDictionary().getSubjects().getNumberOfElements();
			colorTool.log("Checking predicate entries");
			error |= HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "predicate",
					hdt.getDictionary().getPredicates(), console);
			count += hdt.getDictionary().getPredicates().getNumberOfElements();
			colorTool.log("Checking object entries");
			Map<? extends CharSequence, DictionarySection> allObjects = hdt.getDictionary().getAllObjects();
			for (Map.Entry<? extends CharSequence, DictionarySection> entry : allObjects.entrySet()) {
				CharSequence sectionName = entry.getKey();
				DictionarySection section = entry.getValue();
				colorTool.log("Checking object section " + sectionName);
				error |= HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "sectionName", section,
						console);
				count += section.getNumberOfElements();
			}
			colorTool.log("Checking shared entries");
			error |= HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "shared",
					hdt.getDictionary().getShared(), console);
			count += hdt.getDictionary().getShared().getNumberOfElements();
		} else {
			colorTool.log("Checking subject entries");
			error = HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "subject",
					hdt.getDictionary().getSubjects(), console);
			count += hdt.getDictionary().getSubjects().getNumberOfElements();
			colorTool.log("Checking predicate entries");
			error |= HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "predicate",
					hdt.getDictionary().getPredicates(), console);
			count += hdt.getDictionary().getPredicates().getNumberOfElements();
			colorTool.log("Checking object entries");
			error |= HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "object",
					hdt.getDictionary().getObjects(), console);
			count += hdt.getDictionary().getObjects().getNumberOfElements();
			colorTool.log("Checking shared entries");
			error |= HDTVerify.checkDictionarySectionOrder(binary, unicode, colorTool, "shared",
					hdt.getDictionary().getShared(), console);
			count += hdt.getDictionary().getShared().getNumberOfElements();
		}

		if (error) {
			colorTool.error("This HDT isn't valid", true);
		} else {
			colorTool.log(count + " element(s) parsed");
			colorTool.log(colorTool.color(0, 5, 0) + "This HDT is valid", true);
		}

		if (console != null) {
			console.removeLast();
		}
	}

	private long readLong(String str, int startIndex, int endIndex) {
		try {
			return Long.parseLong(str, startIndex, endIndex, 10);
		} catch (Exception e) {
			throw new NumberFormatException("Can't read string " + str.substring(startIndex, endIndex));
		}
	}

	public void executeSail(AbstractNotifyingSail store, Function<Value, Value> valueConverter, Type type)
			throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, UTF_8));

		TripleString triplePattern = new TripleString();

		while (true) {
			System.out.print(colorTool.white() + ">> ");
			System.out.flush();
			String line = in.readLine();
			System.out.print(colorTool.colorReset());
			if (line == null || line.equals("exit") || line.equals("quit")) {
				break;
			}
			if (line.isEmpty() || executeSub(line, type)) {
				continue;
			}
			if (store instanceof EndpointStore ep) {
				String[] params = line.split("\\s");

				if (line.startsWith("bit")) {
					if (params.length != 2) {
						System.err.println(
								colorTool.red() + "bit[dxyz] <index>(:endindex|/range)" + colorTool.colorReset());
						continue;
					}

					Bitmap bm;
					long size;
					long shared = ep.getHdt().getDictionary().getNshared();
					String bmid = params[0].toLowerCase();
					switch (bmid) {
					case "bitx" -> {
						bm = ep.getBitX();
						size = ep.getHdt().getDictionary().getNsubjects();
					}
					case "bity" -> {
						bm = ep.getBitY();
						size = ep.getHdt().getDictionary().getNpredicates();
					}
					case "bitz" -> {
						bm = ep.getBitZ();
						size = ep.getHdt().getDictionary().getNobjects() - shared;
					}
					case "bitd" -> {
						bm = ep.getDeleteBitMap(TripleComponentOrder.SPO);
						size = ep.getHdt().getTriples().getNumberOfElements();
					}
					default -> {
						System.err.println(colorTool.red() + "Unknown bitmap " + params[0] + colorTool.colorReset());
						continue;
					}
					}

					long id;
					long endId;
					try {
						int sliceSymb = params[1].indexOf(':');
						int rangeSymb = params[1].indexOf('/');
						int endRange;
						if (rangeSymb != -1) {
							if (sliceSymb > rangeSymb) {
								System.err.println(colorTool.red() + "Syntax error can't put '/' before ':' in "
										+ params[1] + colorTool.colorReset());
								continue;
							}
							endRange = rangeSymb;
						} else {
							endRange = params[1].length();
						}
						if (sliceSymb == -1) {
							id = endId = readLong(params[1], 0, endRange);
						} else {
							// at least the slice modifier
							id = readLong(params[1], 0, sliceSymb);
							endId = readLong(params[1], sliceSymb + 1, endRange);
						}
						if (rangeSymb != -1) {
							// have the range modifier
							long delta = readLong(params[1], endRange + 1, params[1].length());
							id = Math.min(size, Math.max(1, id - delta));
							endId = Math.max(1, Math.min(size, endId + delta));
						}
					} catch (NumberFormatException e) {
						System.err.println(colorTool.red() + e.getMessage() + colorTool.colorReset());
						continue;
					}

					if (id <= 0 || id > size) {
						System.err.println(
								colorTool.red() + "Index out of bound " + id + " / " + size + colorTool.colorReset());
						continue;
					}
					if (endId <= 0 || endId > size) {
						System.err.println(colorTool.red() + "End index out of bound " + endId + " / " + size
								+ colorTool.colorReset());
						continue;
					}
					if (endId == id) {
						System.out.println(colorTool.yellow() + bmid + colorTool.colorReset() + "[" + colorTool.green()
								+ id + colorTool.colorReset() + "]" + colorTool.colorReset() + " = " + colorTool.white()
								+ (bm.access(id - 1) ? 1 : 0) + colorTool.colorReset());
					} else {
						long shift = 0;
						System.out.println(colorTool.yellow() + bmid + colorTool.colorReset() + "[" + colorTool.green()
								+ id + colorTool.colorReset() + ":" + colorTool.green() + endId + colorTool.colorReset()
								+ "]");
						System.out.print(colorTool.white());
						for (long cid = id; cid <= endId; cid++) {
							System.out.print((bm.access(cid - 1) ? 1 : 0));
							shift++;
							if (shift % 32 == 0) {
								System.out.println();
							} else if (shift % 8 == 0) {
								System.out.print(" ");
							}
						}
						if (shift % 32 != 0) {
							System.out.println();
						}
					}
					continue;
				} else if (params[0].equalsIgnoreCase("integrity")) {
					String opt;
					if (params.length == 1) {
						opt = "help";
					} else {
						opt = params[1];
					}

					switch (opt) {
					case "delta" -> {
						integrityTest(ep);
					}
					case "hdt" -> {
						integrityTest(ep.getHdt());
					}
					case "all" -> {
						integrityTest(ep);
						integrityTest(ep.getHdt());
					}
					default -> {
						colorTool.print(colorTool.red() + "integrity (delta|hdt|all)");
					}
					}
					continue;

				} else if (params[0].equalsIgnoreCase("hdth")) {
					try {
						IteratorTripleString it = ep.getHdt().getHeader().search("", "", "");
						while (it.hasNext()) {
							System.out.println(pretty(it.next()));
						}
					} catch (NotFoundException e) {
						System.err.println(colorTool.red() + "No results found." + colorTool.colorReset());
					}
					continue;
				} else if (params[0].equalsIgnoreCase("hdt")) {
					try {
						parseTriplePattern(triplePattern, line, "hdt ".length());
						iterate(ep.getHdt(), triplePattern.getSubject(), triplePattern.getPredicate(),
								triplePattern.getObject());
					} catch (ParserException e) {
						System.err.println(colorTool.red() + "Could not parse triple pattern: " + e.getMessage()
								+ colorTool.colorReset());
						help(type);
					} catch (NotFoundException e) {
						System.err.println(colorTool.red() + "No results found." + colorTool.colorReset());
					}
					continue;
				} else if (params[0].equalsIgnoreCase("da")) {
					try {
						parseTriplePattern(triplePattern, line, "da ".length());
						iterate(ep.getNativeStoreA(), triplePattern.getSubject(), triplePattern.getPredicate(),
								triplePattern.getObject(), Function.identity());
					} catch (ParserException e) {
						System.err.println(colorTool.red() + "Could not parse triple pattern: " + e.getMessage()
								+ colorTool.colorReset());
						help(type);
					} catch (NotFoundException e) {
						System.err.println(colorTool.red() + "No results found." + colorTool.colorReset());
					}
					continue;
				} else if (params[0].equalsIgnoreCase("db")) {
					try {
						parseTriplePattern(triplePattern, line, "db ".length());
						iterate(ep.getNativeStoreB(), triplePattern.getSubject(), triplePattern.getPredicate(),
								triplePattern.getObject(), Function.identity());
					} catch (ParserException e) {
						System.err.println(colorTool.red() + "Could not parse triple pattern: " + e.getMessage()
								+ colorTool.colorReset());
						help(type);
					} catch (NotFoundException e) {
						System.err.println(colorTool.red() + "No results found." + colorTool.colorReset());
					}
					continue;
				} else if (params[0].equalsIgnoreCase("store")) {
					System.out.println(colorTool.yellow() + "Using store " + (ep.switchStore ? "B" : "A")
							+ colorTool.colorReset());
					continue;
				} else if (params[0].equalsIgnoreCase("tid")) {
					try {
						parseTriplePattern(triplePattern, line, "map ".length());
						System.out.println("Query: " + triplePattern);

						HDTConverter converter = ep.getHdtConverter();
						HDT hdt = ep.getHdt();
						long shared = ep.getHdt().getDictionary().getNshared();

						long sid = hdt.getDictionary().stringToId(triplePattern.getSubject(),
								TripleComponentRole.SUBJECT);
						long pid = hdt.getDictionary().stringToId(triplePattern.getPredicate(),
								TripleComponentRole.PREDICATE);
						long oid = hdt.getDictionary().stringToId(triplePattern.getObject(),
								TripleComponentRole.OBJECT);

						String ms = sid <= 0 ? null : prettyComponent(converter.subjectIdToIRI(sid));
						String mp = pid <= 0 ? null : prettyComponent(converter.predicateIdToIRI(pid));
						String mo = oid <= 0 ? null : prettyComponent(converter.objectIdToIRI(oid));

						System.out.println(colorTool.yellow() + "Subject   : " + colorTool.white() + sid
								+ (sid > 0 && sid <= shared ? " (Shared)" : "") + " "
								+ (ms == null ? "(Not in HDT)" : ms) + colorTool.colorReset());
						System.out.println(colorTool.yellow() + "Predicate : " + colorTool.white() + pid + " "
								+ (mp == null ? "(Not in HDT)" : mp) + colorTool.colorReset());
						System.out.println(colorTool.yellow() + "Object    : " + colorTool.white() + oid
								+ (oid > 0 && oid <= shared ? " (Shared)" : "") + " "
								+ (mo == null ? "(Not in HDT)" : mo) + colorTool.colorReset());
						System.out.println((ms == null ? prettyComponent(triplePattern.getSubject()) : ms) + " "
								+ (ms == null ? prettyComponent(triplePattern.getPredicate()) : mp) + " "
								+ (ms == null ? prettyComponent(triplePattern.getObject()) : mo));
						continue;
					} catch (ParserException e) {
						System.err.println(colorTool.red() + "Could not parse triple pattern: " + e.getMessage()
								+ colorTool.colorReset());
						help(type);
					}
				}
			}
			try {
				parseTriplePattern(triplePattern, line);
				iterate(store, triplePattern.getSubject(), triplePattern.getPredicate(), triplePattern.getObject(),
						valueConverter);
			} catch (ParserException e) {
				System.err.println(
						colorTool.red() + "Could not parse triple pattern: " + e.getMessage() + colorTool.colorReset());
				help(type);
			} catch (NotFoundException e) {
				System.err.println(colorTool.red() + "No results found." + colorTool.colorReset());
			}

		}
	}

	private String pretty(TripleString ts) {
		return prettyComponent(ts.getSubject()) + " " + prettyComponent(ts.getPredicate()) + " "
				+ prettyComponent(ts.getObject());
	}

	private String pretty(Statement ts) {
		return prettyComponent(ts.getSubject()) + " " + prettyComponent(ts.getPredicate()) + " "
				+ prettyComponent(ts.getObject()) + colorTool.colorReset();
	}

	private String prettyComponent(Object object) {
		if (object == null) {
			return colorTool.yellow() + "?";
		}

		String obj = object.toString();
		if (obj.isEmpty()) {
			return "";
		}
		String out;
		if (obj.charAt(0) == '"') {
			int typeIndex = LiteralsUtils.getTypeIndex(obj);
			if (typeIndex == -1) {
				int langIndex = LiteralsUtils.getLangIndex(obj);
				if (langIndex == -1) {
					out = colorTool.green() + obj;
				} else {
					out = colorTool.green() + obj.substring(0, langIndex - 1) + colorTool.white()
							+ obj.substring(langIndex - 1);
				}
			} else {
				out = colorTool.green() + obj.substring(0, typeIndex - 2) + colorTool.white()
						+ obj.substring(typeIndex - 2);
			}
		} else if (obj.charAt(0) == '_') {
			out = colorTool.blue() + obj;
		} else {
			out = colorTool.magenta() + obj;
		}

		if (!(object instanceof Value && showIndex())) {
			return out;
		}

		if (object instanceof SimpleLiteralHDT hdtLit) {
			return out + colorTool.yellow() + " (" + hdtLit.getHDTId() + ")";
		} else if (object instanceof SimpleIRIHDT hdtIri) {
			return out + colorTool.yellow() + " (" + hdtIri.getId() + ")";
		} else if (object instanceof SimpleBNodeHDT hdtBN) {
			return out + colorTool.yellow() + " (" + hdtBN.getHDTId() + ")";
		} else {
			return out + colorTool.yellow();
		}
	}

	public void execute() throws IOException {
		MultiThreadListenerConsole console = new MultiThreadListenerConsole(color);
		colorTool.setConsole(console);
		if (searchCfg != null) {
			configData.setOptions(searchCfg);
		}
		Path path = Path.of(input);
		String type;
		if (this.type == null || this.type.equalsIgnoreCase("guess")) {
			if (Files.isDirectory(path)) {
				if (Files.exists(path.resolve("hdt-store"))) {
					type = "qendpoint";
				} else {
					type = "delta";
				}
			} else if (path.getFileName().toString().endsWith(".hdt")) {
				type = "hdt";
			} else if (path.getFileName().toString().endsWith(".prof")) {
				type = "profiler";
			} else {
				byte[] cookie = FormatUtils.readCookie(path, 8);
				// search for the right magic
				if (Arrays.equals(cookie, "$DltF0\n\r".getBytes(UTF_8))) {
					type = "deltafile";
				} else {
					throw new IllegalArgumentException("Can't guess type for store " + path + "!");
				}
			}
		} else {
			type = this.type.toLowerCase();
		}

		switch (type) {
		case "delta" -> executeDelta();
		case "qendpoint" -> executeDeltaQEndpoint();
		case "hdt" -> executeHDT();
		case "reader" -> executeReader();
		case "profiler" -> executeProfiler();
		case "deltafile" -> executeDeltaFile();
		default -> throw new IllegalArgumentException("Can't understand store of type " + this.type + "!");
		}
	}

	private void executeProfiler() throws IOException {
		try (Profiler p = Profiler.readFromDisk(Path.of(input))) {
			p.setDisabled(false);
			p.writeProfiling();
		}
	}

	private void executeDeltaFile() throws IOException {
		Path file = Path.of(input);
		MultiThreadListenerConsole console = colorTool.getConsole();

		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.PARSER_DELTAFILE_NO_CRC, noCRC,
				HDTOptionsKeys.PARSER_DELTAFILE_NO_EXCEPTION, true);

		try (InputStream stream = new BufferedInputStream(Files.newInputStream(file));
				RDFDeltaFileParser.DeltaFileReader reader = new RDFDeltaFileParser.DeltaFileReader(stream, spec)) {

			console.printLine(console.color(5, 5, 1) + "files .. " + console.colorReset() + reader.getSize());
			console.printLine(console.color(5, 5, 1) + "start .. " + console.colorReset() + reader.getStart());
			console.printLine(console.color(5, 5, 1) + "end .... " + console.colorReset() + reader.getEnd());
			console.printLine(console.color(5, 5, 1) + "flavor . " + console.colorReset() + reader.getFlavor());

			long i = 0;
			long size = reader.getSize();
			while (reader.hasNext()) {
				i++;
				RDFDeltaFileParser.DeltaFileComponent comp = reader.next();

				console.notifyProgress((float) (i * 1000 / size) / 10, "reading files " + i + "/" + size + ": "
						+ console.color(2, 2, 2) + comp.fileName() + console.colorReset());
			}
			if (i != size) {
				console.printLine(console.color(5, 1, 1) + "Error, not everything was read: " + i + " != " + size + " "
						+ (100 * i / size) + "%");
			}
			console.notifyProgress(100, "done");
		}
	}

	private void executeReader() throws IOException {
		colorTool.log("Reader REPL");

		Path pwd = Path.of(input).toAbsolutePath();

		Map<String, Object> loadedEntities = new HashMap<>();

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, UTF_8));
		String line;

		try {
			while ((line = in.readLine()) != null) {
				line = line.trim();

				if (line.isEmpty() || line.charAt(0) == '#') {
					continue;
				}

				String[] args = line.split("\\s");

				try {
					switch (args[0]) {
					case "q", "quit", "exit", "leave" -> {
						colorTool.log("bye.");
						return;
					}
					case "pwd" -> {
						System.out.println(pwd);
					}
					case "ls" -> {
						try (Stream<Path> list = Files.list(pwd)) {
							System.out.println("File for " + pwd);
							list.forEach(path -> System.out.println("- " + path.getFileName()));
						}
					}
					case "cd" -> {
						if (args.length < 2) {
							colorTool.error("cd (path)");
							continue;
						}
						Path npwd = pwd.resolve(args[1]);
						if (!Files.exists(npwd) || !Files.isDirectory(npwd)) {
							colorTool.error(format("the path %s isn't a directory", npwd));
							continue;
						}

						pwd = npwd;
					}
					case "load" -> {
						if (args.length < 4) {
							colorTool.error("load (name) (type) (path) [config]");
							continue;
						}
						String name = args[1].toLowerCase();
						String type = args[2].toLowerCase();
						Path obj = pwd.resolve(args[3]);

						Object ent = loadedEntities.remove(name);
						if (ent != null) {
							colorTool.warn("redefining " + name);
							Closer.closeSingle(ent);
						}

						if (!Files.exists(obj)) {
							colorTool.error(format("the file %s doesn't exist", obj));
							continue;
						}

						switch (type) {
						case "bm", "bitmap" -> {
							if (args.length < 5) {
								colorTool.error("load (name) bitmap (path) (size)");
								continue;
							}

							long size = Long.parseLong(args[4]);
							loadedEntities.put(name, Bitmap64Big.map(obj, size));
							colorTool.log(format("$%s defined", name));
						}
						case "sq", "sequence" -> {
							if (args.length < 5) {
								colorTool.error("load (name) sequence (path) (size) (numbits)");
								continue;
							}
							long size = Long.parseLong(args[4]);
							int numbits = Integer.parseInt(args[5]);

							loadedEntities.put(name, new SequenceLog64BigDisk(obj, numbits, size, true, false));
							colorTool.log(format("$%s defined", name));
						}
						default -> colorTool.error(format("Unknown load type %s", type));
						}
					}
					default -> {
						if (args[0].charAt(0) != '$') {
							colorTool.error("Unknown command", "type help for help");
							continue;
						}

						String vid = args[0].substring(1).toLowerCase();
						Object obj = loadedEntities.get(vid);

						if (obj == null) {
							colorTool.error(format("Can't find the object %s", args[0]));
							continue;
						}

						if (args.length == 1) {
							colorTool.error(format("%s [close]|[id]", args[0]));
							continue;
						}

						switch (args[1]) {
						case "close" -> {
							colorTool.log("object closed");
							Closer.closeSingle(obj);
							loadedEntities.remove(vid);
						}
						case "?" -> {
							if (obj instanceof LongArray la) {
								colorTool.log(format("[array$%d] %s [close]|[id]", la.sizeOf(), args[0]));
								continue;
							} else if (obj instanceof Bitmap) {
								colorTool.log(format("[bitmap] %s [close]|[id]", args[0]));
								continue;
							} else {
								colorTool.error("unknown type: " + obj.getClass());
							}
						}
						default -> {
							long size;
							if (obj instanceof LongArray la) {
								size = la.length();
							} else if (obj instanceof Bitmap bm) {
								size = bm.getNumBits();
							} else {
								colorTool.error("unknown type: " + obj.getClass());
								continue;
							}
							long id;
							long endId;
							try {
								int sliceSymb = args[1].indexOf(':');
								int rangeSymb = args[1].indexOf('/');
								int endRange;
								if (rangeSymb != -1) {
									if (sliceSymb > rangeSymb) {
										System.err.println(colorTool.red() + "Syntax error can't put '/' before ':' in "
												+ args[1] + colorTool.colorReset());
										continue;
									}
									endRange = rangeSymb;
								} else {
									endRange = args[1].length();
								}
								if (sliceSymb == -1) {
									id = endId = readLong(args[1], 0, endRange);
								} else {
									// at least the slice modifier
									id = readLong(args[1], 0, sliceSymb);
									endId = readLong(args[1], sliceSymb + 1, endRange);
								}
								if (rangeSymb != -1) {
									// have the range modifier
									long delta = readLong(args[1], endRange + 1, args[1].length());
									id = Math.min(size, Math.max(1, id - delta));
									endId = Math.max(1, Math.min(size, endId + delta));
								}
							} catch (NumberFormatException e) {
								System.err.println(colorTool.red() + e.getMessage() + colorTool.colorReset());
								continue;
							}

							if (id <= 0 || id > size) {
								System.err.println(colorTool.red() + "Index out of bound " + id + " / " + size
										+ colorTool.colorReset());
								continue;
							}
							if (endId <= 0 || endId > size) {
								System.err.println(colorTool.red() + "End index out of bound " + endId + " / " + size
										+ colorTool.colorReset());
								continue;
							}

							if (obj instanceof LongArray la) {
								if (endId == id) {
									System.out.println(
											colorTool.yellow() + vid + colorTool.colorReset() + "[" + colorTool.green()
													+ id + colorTool.colorReset() + "]" + colorTool.colorReset() + " = "
													+ colorTool.white() + (la.get(id - 1)) + colorTool.colorReset());
								} else {
									long shift = 0;
									System.out.println(colorTool.yellow() + vid + colorTool.colorReset() + "["
											+ colorTool.green() + id + colorTool.colorReset() + ":" + colorTool.green()
											+ endId + colorTool.colorReset() + "]");
									System.out.print(colorTool.white());
									for (long cid = id; cid <= endId; cid++) {
										System.out.print((la.get(cid - 1)));
										shift++;
										if (shift % 32 == 0) {
											System.out.println();
										} else if (shift % 8 == 0) {
											System.out.print(" ");
										}
									}
									if (shift % 32 != 0) {
										System.out.println();
									}
								}
							} else {
								Bitmap bm = (Bitmap) obj;
								if (endId == id) {
									System.out.println(colorTool.yellow() + vid + colorTool.colorReset() + "["
											+ colorTool.green() + id + colorTool.colorReset() + "]"
											+ colorTool.colorReset() + " = " + colorTool.white()
											+ (bm.access(id - 1) ? 1 : 0) + colorTool.colorReset());
								} else {
									long shift = 0;
									System.out.println(colorTool.yellow() + vid + colorTool.colorReset() + "["
											+ colorTool.green() + id + colorTool.colorReset() + ":" + colorTool.green()
											+ endId + colorTool.colorReset() + "]");
									System.out.print(colorTool.white());
									for (long cid = id; cid <= endId; cid++) {
										System.out.print((bm.access(cid - 1) ? 1 : 0));
										shift++;
										if (shift % 32 == 0) {
											System.out.println();
										} else if (shift % 8 == 0) {
											System.out.print(" ");
										}
									}
									if (shift % 32 != 0) {
										System.out.println();
									}
								}
							}

						}
						}

					}
					}
				} catch (Exception e) {
					colorTool.error("Error while running command", e.getMessage());
				}
			}
		} finally {
			Closer.closeAll(loadedEntities);
		}
	}

	public static void main(String[] args) throws Throwable {
		QEPSearch qepSearch = new QEPSearch();
		JCommander com = new JCommander(qepSearch);
		com.parse(args);
		com.setProgramName("qepSearch");
		qepSearch.colorTool = new ColorTool(qepSearch.color, qepSearch.quiet);

		if (showVersion) {
			System.out.println(HDTVersion.get_version_string("."));
			System.exit(0);
		}

		if (qepSearch.parameters.size() != 1) {
			com.usage();
			System.exit(1);
		}

		qepSearch.input = qepSearch.parameters.get(0);

		qepSearch.execute();
	}

}
