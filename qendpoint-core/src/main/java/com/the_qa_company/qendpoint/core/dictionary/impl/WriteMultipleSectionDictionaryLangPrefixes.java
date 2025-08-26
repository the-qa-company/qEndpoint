package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.WritePFCDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIteratorImpl;
import com.the_qa_company.qendpoint.core.iterator.utils.PipedCopyIterator;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.PrefixesStorage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Version of mutli-section dictionary with {@link WritePFCDictionarySection}
 *
 * @author Antoine Willerval
 */
public class WriteMultipleSectionDictionaryLangPrefixes extends MultipleLangBaseDictionary {
	protected final PrefixesStorage prefixesStorage;
	private final Path filename;
	private final int bufferSize;

	private static HDTOptions withoutRDFType(HDTOptions opt) {
		HDTOptions spec = opt.pushTop();
		spec.set(HDTOptionsKeys.DICTIONARY_MSDL_NO_RDFTYPE_INDEX, true);
		return spec;
	}

	public WriteMultipleSectionDictionaryLangPrefixes(HDTOptions spec, Path filename, int bufferSize) {
		this(spec, filename, bufferSize, false);
	}

	public WriteMultipleSectionDictionaryLangPrefixes(HDTOptions spec, Path filename, int bufferSize, boolean quad) {
		super(withoutRDFType(spec));
		this.filename = filename;
		this.bufferSize = bufferSize;
		String name = filename.getFileName().toString();
		subjects = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "SU"), bufferSize);
		predicates = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "PR"),
				bufferSize);
		typed = new TreeMap<>();
		nonTyped = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "NT"), bufferSize);
		shared = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "SH"), bufferSize);
		languages = new TreeMap<>();
		if (quad) {
			graph = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "GR"), bufferSize);
		}
		prefixesStorage = new PrefixesStorage();
		prefixesStorage.loadConfig(spec.get(HDTOptionsKeys.LOADER_PREFIXES));
	}

	public WriteMultipleSectionDictionaryLangPrefixes(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, DictionarySectionPrivate shared,
			TreeMap<ByteString, DictionarySectionPrivate> objects, PrefixesStorage prefixesStorage) {
		this(spec, subjects, predicates, shared, objects, null, prefixesStorage);
	}

	public WriteMultipleSectionDictionaryLangPrefixes(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, DictionarySectionPrivate shared,
			TreeMap<ByteString, DictionarySectionPrivate> objects) {
		this(spec, subjects, predicates, shared, objects, null, null);
	}

	public WriteMultipleSectionDictionaryLangPrefixes(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, DictionarySectionPrivate shared,
			TreeMap<ByteString, DictionarySectionPrivate> objects, DictionarySectionPrivate graph,
			PrefixesStorage prefixesStorage) {
		super(spec);
		// useless
		this.filename = null;
		this.bufferSize = 0;

		// write sections
		this.subjects = subjects;
		this.predicates = predicates;
		this.typed = new TreeMap<>();
		this.languages = new TreeMap<>();
		this.shared = shared;
		this.graph = graph;
		for (var e : objects.entrySet()) {
			ByteString type = e.getKey();
			DictionarySectionPrivate sec = e.getValue();
			if (type.isEmpty()) {
				throw new IllegalArgumentException("empty type");
			}
			if (type.charAt(0) == '@') {
				languages.put(type.subSequence(1), sec);
			} else if (!type.equals(LiteralsUtils.NO_DATATYPE)) {
				this.typed.put(type, sec);
			} else {
				assert nonTyped == null : "Already defined NDT section";
				this.nonTyped = sec;
			}
		}
		this.prefixesStorage = prefixesStorage;
	}

	private ExceptionThread fillSection(Iterator<? extends CharSequence> objects, long count,
			ProgressListener listener) {
		@SuppressWarnings("resource")
		PipedCopyIterator<TypedByteString> datatypeIterator = new PipedCopyIterator<>();
		String name = filename.getFileName().toString();
		Map<ByteString, DictionarySectionPrivate> theTyped = Collections.synchronizedMap(this.typed);
		Map<ByteString, DictionarySectionPrivate> theLanguages = Collections.synchronizedMap(this.languages);
		return new ExceptionThread(() -> {
			// object reader
			try {
				ByteString oldType = null;
				boolean oldTypeLang = false;
				long block = count < 10 ? 1 : count / 10;
				long currentCount = 0;
				for (; objects.hasNext(); currentCount++) {
					ByteString next = (ByteString) objects.next();

					ByteString lit = LiteralsUtils.prefToLitLang(next);
					ByteString type = (ByteString) LiteralsUtils.getType(lit);
					boolean lang = type == LiteralsUtils.LITERAL_LANG_TYPE;

					if (lang) {
						type = (ByteString) LiteralsUtils.getLanguage(lit).orElseThrow();
					}

					if (currentCount % block == 0) {
						listener.notifyProgress((float) (currentCount * 100 / count), "Filling section");
					}

					if (oldType != null) {
						if (oldType.equals(type) && lang == oldTypeLang) {
							datatypeIterator.addElement(new TypedByteString(oldType,
									(ByteString) LiteralsUtils.removeTypeAndLang(lit), oldTypeLang));
							continue;
						} else {
							datatypeIterator.closePipe();
						}
					}
					oldType = type;
					oldTypeLang = lang;

					datatypeIterator.addElement(new TypedByteString(oldType,
							(ByteString) LiteralsUtils.removeTypeAndLang(lit), oldTypeLang));
				}
				datatypeIterator.closePipe();
				datatypeIterator.closePipe();
			} catch (Throwable e) {
				try {
					throw e;
				} finally {
					datatypeIterator.closePipe(e);
				}
			}
		}, "MultiSecSAsyncObjectReader").attach(new ExceptionThread(() -> {
			// datatype writer
			PeekIteratorImpl<TypedByteString> dataTypePeekIt = new PeekIteratorImpl<>(datatypeIterator);
			// section id to not having to write an URI on disk
			Map<ByteString, Long> sectionIds = new HashMap<>();

			// check that we have at least one element to read

			BufferedWriter debugWriter = null;

			Path writePath = spec.getPath("debug.msdl.write");
			if (writePath != null) {
				debugWriter = Files.newBufferedWriter(writePath);
			}

			try {
				while (dataTypePeekIt.hasNext()) {
					TypedByteString typedBS = dataTypePeekIt.peek();
					ByteString type = typedBS.type();
					ByteString utype;
					if (typedBS.lang) {
						utype = LiteralsUtils.LANG_OPERATOR.copyAppend(type);
					} else {
						utype = type;
					}
					Long sid = sectionIds.get(utype);
					if (debugWriter != null) {
						debugWriter.append(utype).append(" -> ").append(typedBS.node).append("\n");
						debugWriter.flush();
					}
					if (sid != null) {
						// check that the section wasn't already defined
						throw new IllegalArgumentException("type " + utype + " is already defined");
					}
					// create a new id
					long sidNew = 1L + sectionIds.size();
					sectionIds.put(type, sidNew);

					// create the new section

					DictionarySectionPrivate section;
					if (type == LiteralsUtils.NO_DATATYPE && !typedBS.lang) {
						section = nonTyped;
					} else {
						section = DictionarySectionFactory.createWriteSection(spec,
								filename.resolveSibling(name + (typedBS.lang ? "lang" : "type") + sidNew), bufferSize);

						if (typedBS.lang) {
							theLanguages.put(type, section);
						} else {
							theTyped.put(type, section);
						}
					}
					section.load(dataTypePeekIt.map(TypedByteString::node), count, null);

					// reset the pipe to allow reading more elements
					((PipedCopyIterator<?>) dataTypePeekIt.getWrappedIterator()).reset();
				}
			} catch (Throwable t) {
				IOUtil.closeQuietly(debugWriter);
				throw t;
			}
		}, "MultiSecSAsyncObjectDatatypeWriter"));
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		MultiThreadListener ml = ListenerUtil.multiThreadListener(listener);
		ml.unregisterAllThreads();
		ExceptionThread.async("MultiSecSAsyncReader",
				() -> predicates.load(other.getPredicates(), new IntermediateListener(ml, "Predicate: ")), () -> {
					if (supportGraphs()) {
						graph.load(other.getGraphs(), new IntermediateListener(ml, "Graph:      "));
					}
				}, () -> subjects.load(other.getSubjects(), new IntermediateListener(ml, "Subjects:  ")),
				() -> shared.load(other.getShared(), new IntermediateListener(ml, "Shared:    ")))
				.attach(fillSection(other.getObjects().getEntries(), other.getObjects().getNumberOfElements(),
						new IntermediateListener(ml, "Objects:   ")))
				.startAll().joinAndCrashIfRequired();
		ml.unregisterAllThreads();
	}

	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(getType());
		ci.setInt("elements", this.getNumberOfElements());
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		prefixesStorage.save(output, iListener);

		iListener.setRange(0, 20);
		iListener.setPrefix("Save shared: ");
		shared.save(output, iListener);
		iListener.setRange(20, 40);
		iListener.setPrefix("Save subjects: ");
		subjects.save(output, iListener);
		iListener.setRange(40, 60);
		iListener.setPrefix("Save predicates: ");
		predicates.save(output, iListener);

		iListener.setRange(60, 80);
		iListener.setPrefix("Save non typed objects: ");
		nonTyped.save(output, iListener);

		int rangeStart;
		if (supportGraphs()) {
			iListener.setRange(80, 85);
			iListener.setPrefix("Save graphs: ");
			graph.save(output, listener);
			rangeStart = 85;
		} else {
			rangeStart = 80;
		}
		iListener.setRange(rangeStart, 100);
		iListener.setPrefix("Save objects: ");

		int count = typed.size() + languages.size();
		VByte.encode(output, count);

		float node = 20f / count;
		float percentage = 80;

		for (var entry : typed.entrySet()) {
			output.write(SECTION_TYPE_DT);
			iListener.setRange(percentage, percentage + node);
			percentage += node;
			IOUtil.writeSizedBuffer(output, entry.getKey(), iListener);
			entry.getValue().save(output, iListener);
		}

		for (var entry : languages.entrySet()) {
			output.write(SECTION_TYPE_LANG);
			iListener.setRange(percentage, percentage + node);
			percentage += node;
			IOUtil.writeSizedBuffer(output, entry.getKey(), iListener);
			entry.getValue().save(output, iListener);
		}

	}

	@Override
	public String getType() {
		if (supportGraphs()) {
			throw new NotImplementedException();
		}
		return HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION_LANG_PREFIXES;
	}

	@Override
	public PrefixesStorage getPrefixesStorage(boolean ignoreMapping) {
		return prefixesStorage;
	}

	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void load(TempDictionary other, ProgressListener listener) {
		throw new NotImplementedException();
	}

	private record TypedByteString(ByteString type, ByteString node, boolean lang) {}
}
