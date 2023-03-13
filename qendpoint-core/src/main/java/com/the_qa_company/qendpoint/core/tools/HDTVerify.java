package com.the_qa_company.qendpoint.core.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.impl.MultipleBaseDictionary;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
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

	@Parameter(names = "-equals", description = "Test all the input HDTs are equals instead of checking validity")
	public boolean equals;

	public ColorTool colorTool;

	private HDT loadOrMap(String file, ProgressListener listener) throws IOException {
		return load ? HDTManager.loadHDT(file, listener) : HDTManager.mapHDT(file, listener);
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
			colorTool.warn("Not valid section");
		} else {
			colorTool.log("valid section");
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

		IteratorTripleString its1;
		IteratorTripleString its2;

		try {
			its1 = hdt1.search("", "", "");
			its2 = hdt2.search("", "", "");
		} catch (NotFoundException e) {
			throw new AssertionError(e);
		}

		long tripleError = 0;
		long count = 0;
		long size = hdt1.getTriples().getNumberOfElements();
		while (true) {
			if (!its1.hasNext()) {
				if (its2.hasNext()) {
					colorTool.error("Bad iteration");
					break;
				}
				return true;
			}

			if (!its2.hasNext()) {
				colorTool.error("Bad iteration");
				return false;
			}

			TripleString ts1 = its1.next();
			TripleString ts2 = its2.next();
			if (!ts1.equals(ts2)) {
				colorTool.error("Triple not equal!", ts1 + "!=" + ts2);
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

		try {
			for (String hdtLocation : parameters) {
				hdts.add(loadOrMap(hdtLocation, console));
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
						boolean error;
						long count = 0;
						if (hdt.getDictionary() instanceof MultipleBaseDictionary) {
							colorTool.log("Checking subject entries");
							error = checkDictionarySectionOrder(binary, unicode, colorTool, "subject",
									hdt.getDictionary().getSubjects(), console);
							count += hdt.getDictionary().getSubjects().getNumberOfElements();
							colorTool.log("Checking predicate entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "predicate",
									hdt.getDictionary().getPredicates(), console);
							count += hdt.getDictionary().getPredicates().getNumberOfElements();
							colorTool.log("Checking object entries");
							Map<? extends CharSequence, DictionarySection> allObjects = hdt.getDictionary()
									.getAllObjects();
							for (Map.Entry<? extends CharSequence, DictionarySection> entry : allObjects.entrySet()) {
								CharSequence sectionName = entry.getKey();
								DictionarySection section = entry.getValue();
								colorTool.log("Checking object section " + sectionName);
								error |= checkDictionarySectionOrder(binary, unicode, colorTool, "sectionName", section,
										console);
								count += section.getNumberOfElements();
							}
							colorTool.log("Checking shared entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "shared",
									hdt.getDictionary().getShared(), console);
							count += hdt.getDictionary().getShared().getNumberOfElements();
						} else {
							colorTool.log("Checking subject entries");
							error = checkDictionarySectionOrder(binary, unicode, colorTool, "subject",
									hdt.getDictionary().getSubjects(), console);
							count += hdt.getDictionary().getSubjects().getNumberOfElements();
							colorTool.log("Checking predicate entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "predicate",
									hdt.getDictionary().getPredicates(), console);
							count += hdt.getDictionary().getPredicates().getNumberOfElements();
							colorTool.log("Checking object entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "object",
									hdt.getDictionary().getObjects(), console);
							count += hdt.getDictionary().getObjects().getNumberOfElements();
							colorTool.log("Checking shared entries");
							error |= checkDictionarySectionOrder(binary, unicode, colorTool, "shared",
									hdt.getDictionary().getShared(), console);
							count += hdt.getDictionary().getShared().getNumberOfElements();
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
