package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.PrefixesStorage;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HDTVerify {

	private HDTVerify() {
	}

	@Parameter(description = "<input HDTs>")
	public List<String> parameters = Lists.newArrayList();

	@Parameter(names = "-unicode", description = "Ignore UNICODE order")
	public boolean unicode;

	@Parameter(names = "-progress", description = "Show progression")
	public boolean progress;

	@Parameter(names = "-color", description = "Print using color (if available)")
	public boolean color;

	@Parameter(names = "-binary", description = "Print binaries of the string in case of signum error")
	public boolean binary;

	@Parameter(names = "-quiet", description = "Do not show progress of the conversion")
	public boolean quiet;

	@Parameter(names = "-load", description = "Load the HDT in memory for faster results (might be impossible for large a HDT)")
	public boolean load;

	@Parameter(names = "-shared", description = "Check shared section")
	public boolean shared;

	@Parameter(names = "-options", description = "options")
	public String options;

	@Parameter(names = "-equals", description = "Test all the input HDTs are equals instead of checking validity")
	public boolean equals;

	public ColorTool colorTool;

	private HDT loadOrMap(String file, ProgressListener listener, HDTOptions spec) throws IOException {
		return load ? HDTManager.loadHDT(file, listener, spec) : HDTManager.mapHDT(file, listener, spec);
	}

	private static void print(byte[] arr) {
		for (byte b : arr) {
			System.out.printf("%02X ", b);
		}
		System.out.println();
	}

	private static void print(CharSequence seq) {
		if (seq instanceof CompactString cs1) {
			print(cs1.getData());
		}

		if (seq instanceof String rs1) {
			print(rs1.getBytes());
		}
	}

	public static boolean checkDictionarySectionOrder(boolean binary, boolean unicode, ColorTool colorTool, String name,
			DictionarySection section, MultiThreadListenerConsole console) {
		return checkDictionarySectionOrder(binary, unicode, colorTool, name, section, console, null);
	}

	public static boolean checkDictionarySharedSectionOrder(boolean binary, boolean unicode, ColorTool colorTool,
			Dictionary dict, MultiThreadListenerConsole console) {
		DictionarySection shared = dict.getShared();
		Iterator<? extends CharSequence> itsh = shared.getSortedEntries();
		IntermediateListener il = new IntermediateListener(console);
		il.setPrefix("shared: ");

		boolean error = false;
		long countSh = 0;
		long sizeSh = shared.getNumberOfElements();
		DictionarySection subject = dict.getSubjects();
		DictionarySection ndt = dict.getAllObjects().get(LiteralsUtils.NO_DATATYPE);
		while (itsh.hasNext()) {
			ByteString cs = ByteString.of(itsh.next());

			long sid = subject.locate(cs);
			long oid;
			if (ndt != null) {
				oid = ndt.locate(cs);
			} else {
				oid = 0;
			}

			if (sid >= 1) {
				if (oid >= 1) {
					colorTool.error("Find node in shared sec in subject/object", cs.toString());
				} else {
					colorTool.error("Find node in shared sec in subject", cs.toString());
				}
				error = true;
			} else if (oid >= 1) {
				colorTool.error("Find node in shared sec in object", cs.toString());
			}

			countSh++;

			if (countSh % 10_000 == 0) {
				String str = cs.toString();
				il.notifyProgress(100f * countSh / sizeSh, "Verify shared (" + countSh + "/" + sizeSh + "): "
						+ colorTool.color(3, 3, 3) + (str.length() > 17 ? (str.substring(0, 17) + "...") : str));
			}
		}
		if (error)
			return true;

		il.setPrefix("shared2: ");

		Iterator<? extends CharSequence> itsu = subject.getSortedEntries();
		long sizeSu = subject.getNumberOfElements();

		int countSu = 0;

		if (ndt == null) {
			colorTool.log("no no datatype object section");
		} else {
			while (itsu.hasNext()) {
				ByteString cs = ByteString.of(itsu.next());

				if (ndt.locate(cs) > 0) {
					colorTool.error("Find subct node in object section", cs.toString());
				}

				countSu++;

				if (countSu % 10_000 == 0) {
					String str = cs.toString();
					il.notifyProgress(100f * countSu / sizeSu, "Verify subject (" + countSu + "/" + sizeSu + "): "
							+ colorTool.color(3, 3, 3) + (str.length() > 17 ? (str.substring(0, 17) + "...") : str));
				}
			}
		}

		return error;
	}

	public static boolean checkDictionarySectionOrder(boolean binary, boolean unicode, ColorTool colorTool, String name,
			DictionarySection section, MultiThreadListenerConsole console, PrefixesStorage prefixes) {
		Iterator<? extends CharSequence> it = section.getSortedEntries();
		long size = section.getNumberOfElements();
		IntermediateListener il = new IntermediateListener(console);
		il.setPrefix(name + ": ");
		ReplazableString prev = new ReplazableString();
		String lastStr = "";
		boolean error = false;
		long count = 0;
		while (it.hasNext()) {
			ByteString charSeq = ByteString.of(it.next());
			if (prefixes != null) {
				if (!charSeq.isEmpty()) {
					char c0 = charSeq.charAt(0);
					if (c0 != LiteralsUtils.DATATYPE_PREFIX_BYTE && c0 != '"' && c0 != '_') {
						colorTool.error("Non prefixed val data", String.valueOf(charSeq));
					}
				}
				int pref = prefixes.prefixOf(charSeq);
				if ((pref & 1) != 0) {
					colorTool.error("Non prefixed val", charSeq + " pref " + (pref >> 1));
				}
			}
			String str = charSeq.toString();
			count++;

			int cmp = prev.compareTo(charSeq);

			if (cmp >= 0) {
				error = true;
				if (cmp == 0) {
					colorTool.error("Duplicated(bs)", prev + " == " + charSeq);
				} else {
					colorTool.error("Bad order(bs)", prev + " > " + charSeq);
				}
			}

			if (!unicode) {
				int cmp2 = lastStr.compareTo(str);

				if (cmp2 >= 0) {
					error = true;
					if (cmp == 0) {
						colorTool.error("Duplicated(str)", lastStr + " == " + str);
					} else {
						colorTool.error("Bad order(str)", lastStr + " > " + str);
					}
				}

				if (Math.signum(cmp) != Math.signum(cmp2)) {
					error = true;
					colorTool.error("Not equal", cmp + " != " + cmp2 + " for " + lastStr + " / " + str);
					if (binary) {
						print(prev);
						print(charSeq);
						print(lastStr);
						print(str);
					}
				}

				lastStr = str;
			}

			if (count % 10_000 == 0) {
				il.notifyProgress(100f * count / size, "Verify (" + count + "/" + size + "): "
						+ colorTool.color(3, 3, 3) + (str.length() > 17 ? (str.substring(0, 17) + "...") : str));
			}

			prev.replace(charSeq);
		}
		il.notifyProgress(100f, "Verify...");

		if (error) {
			colorTool.warn("Not valid section" + (prefixes != null ? " (prefix)" : "") + " " + size + " nodes");
		} else {
			colorTool.log("valid section" + (prefixes != null ? " (prefix)" : "") + " " + size + " nodes");
		}
		return error;
	}

	public boolean assertHdtEquals(HDT hdt1, HDT hdt2, MultiThreadListenerConsole console, String desc) {
		IntermediateListener il = new IntermediateListener(console);
		il.setPrefix(desc + ": ");
		if (hdt1.getTriples().getNumberOfElements() != hdt2.getTriples().getNumberOfElements()) {
			colorTool.error("HDT with different number of elements!");
			return false;
		}

		IteratorTripleID iti1 = hdt1.getTriples().searchAll();
		IteratorTripleID iti2 = hdt2.getTriples().searchAll();

		long tripleError = 0;
		long count = 0;
		long size = hdt1.getTriples().getNumberOfElements();
		while (true) {
			if (!iti1.hasNext()) {
				if (iti2.hasNext()) {
					colorTool.error("Bad iteration");
					break;
				}
				return true;
			}

			if (!iti2.hasNext()) {
				colorTool.error("Bad iteration");
				return false;
			}

			TripleID ti1 = iti1.next();
			TripleID ti2 = iti2.next();
			TripleString ts1 = hdt1.getDictionary().toTripleString(ti1);
			TripleString ts2 = hdt1.getDictionary().toTripleString(ti2);

			if (!ti1.equals(ti2)) {
				colorTool.error("TripleID not equal!", ti1 + "!=" + ti2 + " / " + ts1 + ":" + ts2);
				tripleError++;
			}

			if (!ts1.equals(ts2)) {
				colorTool.error("TripleString not equal!", ts1 + "!=" + ts2 + " / " + ti1 + "!=" + ti2);
				tripleError++;
			}

			count++;

			if (count % 10_000 == 0) {
				String str = ts1.toString();
				il.notifyProgress(100f * count / size, "Verify (" + count + "/" + size + "): "
						+ colorTool.color(3, 3, 3) + (str.length() > 17 ? (str.substring(0, 17) + "...") : str));
			}
		}

		return tripleError == 0;
	}

	public void exec() throws Throwable {
		MultiThreadListenerConsole console = progress ? new MultiThreadListenerConsole(color) : null;
		colorTool.setConsole(console);
		List<HDT> hdts = new ArrayList<>(parameters.size());

		HDTOptions spec = options != null ? HDTOptions.of(options) : HDTOptions.empty();

		try {
			for (String hdtLocation : parameters) {
				hdts.add(loadOrMap(hdtLocation, console, spec));
			}
			if (equals) {
				// we know that we have at least one HDT
				HDT current = hdts.get(0);

				boolean error = false;
				for (int i = 1; i < hdts.size(); i++) {
					if (!assertHdtEquals(current, hdts.get(i), console, "#0?" + i)) {
						colorTool.error("HDT NOT EQUALS!", "hdt#0 != hdt#" + i);
						error = true;
					}
				}

				if (error) {
					colorTool.error("HDTs not equal!", true);
					System.exit(-1);
				} else {
					colorTool.log(colorTool.color(0, 5, 0) + "All the HDTs are equal", true);
				}

				if (console != null) {
					console.removeLast();
				}

			} else {
				for (HDT hdtl : hdts) {
					try (HDT hdt = hdtl) {
						boolean error = false;
						long count = 0;

						// check shared section
						if (this.shared) {
							error |= checkDictionarySharedSectionOrder(binary, unicode, colorTool, hdt.getDictionary(),
									console);
						}

						PrefixesStorage prefixes = hdt.getDictionary().isPrefixDictionary()
								? hdt.getDictionary().getPrefixesStorage(true)
								: null;
						if (prefixes != null) {
							colorTool.logValue("Prefixes:", prefixes.saveConfig());
						}
						if (hdt.getDictionary().isMultiSectionDictionary()) {
							colorTool.log("Checking subject entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "subject",
									hdt.getDictionary().getSubjects(), console, prefixes);
							count += hdt.getDictionary().getSubjects().getNumberOfElements();
							colorTool.log("Checking predicate entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "predicate",
									hdt.getDictionary().getPredicates(), console, prefixes);
							count += hdt.getDictionary().getPredicates().getNumberOfElements();
							colorTool.log("Checking object entries");
							Map<? extends CharSequence, DictionarySection> allObjects = hdt.getDictionary()
									.getAllObjects();
							for (Map.Entry<? extends CharSequence, DictionarySection> entry : allObjects.entrySet()) {
								CharSequence sectionName = entry.getKey();
								DictionarySection section = entry.getValue();
								colorTool.log("Checking object section " + sectionName);
								error |= checkDictionarySectionOrder(binary, unicode, colorTool, "sectionName", section,
										console, prefixes);
								count += section.getNumberOfElements();
							}
							colorTool.log("Checking shared entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "shared",
									hdt.getDictionary().getShared(), console, prefixes);
							count += hdt.getDictionary().getShared().getNumberOfElements();
						} else {
							colorTool.log("Checking subject entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "subject",
									hdt.getDictionary().getSubjects(), console, prefixes);
							count += hdt.getDictionary().getSubjects().getNumberOfElements();
							colorTool.log("Checking predicate entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "predicate",
									hdt.getDictionary().getPredicates(), console, prefixes);
							count += hdt.getDictionary().getPredicates().getNumberOfElements();
							colorTool.log("Checking object entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "object",
									hdt.getDictionary().getObjects(), console, prefixes);
							count += hdt.getDictionary().getObjects().getNumberOfElements();
							colorTool.log("Checking shared entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "shared",
									hdt.getDictionary().getShared(), console, prefixes);
							count += hdt.getDictionary().getShared().getNumberOfElements();
						}
						if (hdt.getDictionary().supportGraphs()) {
							colorTool.log("Checking graph entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "graph",
									hdt.getDictionary().getGraphs(), console, prefixes);
							count += hdt.getDictionary().getGraphs().getNumberOfElements();
						}

						if (error) {
							colorTool.error("This HDT isn't valid", true);
							System.exit(-1);
						} else {
							colorTool.log(count + " element(s) parsed");
							colorTool.log(colorTool.color(0, 5, 0) + "This HDT is valid", true);
						}

						if (console != null) {
							console.removeLast();
						}
					}
				}
			}
		} catch (Throwable t) {
			IOUtil.closeAll(hdts);
			throw t;
		}

	}

	public static void main(String[] args) throws Throwable {
		HDTVerify verify = new HDTVerify();
		JCommander com = new JCommander(verify);
		com.parse(args);
		verify.colorTool = new ColorTool(verify.color, verify.quiet);
		com.setProgramName("hdtVerify");
		if (verify.parameters.size() < 1) {
			com.usage();
			System.exit(-1);
		}
		verify.exec();
	}
}
