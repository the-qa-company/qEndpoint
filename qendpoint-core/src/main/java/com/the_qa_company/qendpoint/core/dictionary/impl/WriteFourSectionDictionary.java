package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.WriteDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Version of four section dictionary with {@link WriteDictionarySection}
 *
 * @author Antoine Willerval
 */
public class WriteFourSectionDictionary extends BaseDictionary {
	public WriteFourSectionDictionary(HDTOptions spec, Path filename, int bufferSize, boolean quads) {
		super(spec);
		String name = filename.getFileName().toString();
		subjects = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "SU"), bufferSize);
		predicates = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "PR"),
				bufferSize);
		objects = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "OB"), bufferSize);
		shared = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "SH"), bufferSize);

		if (quads) {
			graphs = DictionarySectionFactory.createWriteSection(spec, filename.resolveSibling(name + "GH"),
					bufferSize);
		}
	}

	public WriteFourSectionDictionary(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, DictionarySectionPrivate objects, DictionarySectionPrivate shared) {
		this(spec, subjects, predicates, objects, shared, null);
	}

	public WriteFourSectionDictionary(HDTOptions spec, DictionarySectionPrivate subjects,
			DictionarySectionPrivate predicates, DictionarySectionPrivate objects, DictionarySectionPrivate shared,
			DictionarySectionPrivate graph) {
		super(spec);
		this.subjects = subjects;
		this.predicates = predicates;
		this.objects = objects;
		this.shared = shared;
		this.graphs = graph;
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		MultiThreadListener ml = ListenerUtil.multiThreadListener(listener);
		ml.unregisterAllThreads();
		ExceptionThread.async("FourSecSAsyncReader",
				() -> predicates.load(other.getPredicates(), new IntermediateListener(ml, "Predicate: ")),
				() -> subjects.load(other.getSubjects(), new IntermediateListener(ml, "Subjects:  ")), () -> {
					if (supportGraphs()) {
						graphs.load(other.getGraphs(), new IntermediateListener(ml, "Graph:      "));
					}
				}, () -> shared.load(other.getShared(), new IntermediateListener(ml, "Shared:    ")),
				() -> objects.load(other.getObjects(), new IntermediateListener(ml, "Object:    "))).startAll()
				.joinAndCrashIfRequired();
		ml.unregisterAllThreads();
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

	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(getType());
		ci.setInt("elements", this.getNumberOfElements());
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		iListener.setRange(0, 25);
		iListener.setPrefix("Save shared: ");
		shared.save(output, iListener);
		iListener.setRange(25, 50);
		iListener.setPrefix("Save subjects: ");
		subjects.save(output, iListener);
		iListener.setRange(50, 75);
		iListener.setPrefix("Save predicates: ");
		predicates.save(output, iListener);

		int rangeStart = supportGraphs() ? 77 : 100;
		iListener.setRange(75, rangeStart);
		iListener.setPrefix("Save objects: ");
		objects.save(output, iListener);
		if (supportGraphs()) {
			iListener.setRange(rangeStart, 100);
			iListener.setPrefix("Save graphs: ");
			graphs.save(output, listener);
		}
	}

	@Override
	public void populateHeader(Header header, String rootNode) {
		header.insert(rootNode, HDTVocabulary.DICTIONARY_TYPE, getType());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSHARED, getNshared());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_SIZE_STRINGS, size());
	}

	@Override
	public String getType() {
		return supportGraphs() ? HDTVocabulary.DICTIONARY_TYPE_FOUR_QUAD_SECTION
				: HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION;
	}

	@Override
	public boolean supportGraphs() {
		return graphs != null;
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(shared, subjects, predicates, objects, graphs);
	}
}
