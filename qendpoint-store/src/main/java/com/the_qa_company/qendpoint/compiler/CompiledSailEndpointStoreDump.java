package com.the_qa_company.qendpoint.compiler;

import com.the_qa_company.qendpoint.store.EndpointStore;
import com.the_qa_company.qendpoint.store.EndpointStoreDump;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.sail.lucene.LuceneSail;
import org.eclipse.rdf4j.sail.lucene.impl.LuceneIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

public class CompiledSailEndpointStoreDump extends EndpointStoreDump.EndpointStoreDumpDataset {
	private static final Logger logger = LoggerFactory.getLogger(CompiledSailEndpointStoreDump.class);
	private final CompiledSail compiledSail;

	public CompiledSailEndpointStoreDump(Path outputLocation, CompiledSail compiledSail) {
		super(outputLocation);
		this.compiledSail = compiledSail;
	}

	@Override
	public void beforeMerge(EndpointStore store) throws IOException {
		// dump the Lucene dataset
		Lock lock = store.getLocksNotify().createLock("merge-dumplucene");
		StringBuilder infoWriter = new StringBuilder();
		Path out = outputLocation.resolve("lucene");
		try {
			int ukn = 0;
			Set<String> names = new HashSet<>();
			for (LuceneSail ls : compiledSail.getLuceneSails()) {
				if (!(ls.getLuceneIndex() instanceof LuceneIndex li)) {
					logger.error("Can't dump index {}", ls.getLuceneIndex());
					continue;
				}
				li.getIndexWriter().flush();

				// find a unique name for the lucene dir
				String configDir = ls.getParameter("lucenedir");
				String name;
				if (configDir == null || configDir.isEmpty()) {
					name = "ukn_" + ukn++;
				} else {
					name = Path.of(configDir).getFileName().toString();
				}

				String tname = name;
				int dif = 1;
				while (!names.add(tname)) {
					tname = name + "." + dif++;
				}
				Path outputDataset = out.resolve(tname);

				// write dataset metadata
				infoWriter.append("location=%s\noutput=%s\n---\n".formatted(configDir, outputDataset));

				// clone the index
				Files.createDirectories(outputDataset);
				Directory lsdir = li.getDirectory();
				try (FSDirectory dir = FSDirectory.open(outputDataset)) {
					for (String file : lsdir.listAll()) {
						dir.copyFrom(lsdir, file, file, new IOContext());
					}
				}
			}
			Files.writeString(outputLocation.resolve("lucene.info"), infoWriter.toString());
		} finally {
			lock.release();
		}
		super.beforeMerge(store);
	}
}
