package com.the_qa_company.qendpoint.core.merge;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HDTMergeJoinIteratorTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	private InputStream getStream(String filename) {
		InputStream is = getClass().getResourceAsStream(filename);
		Assert.assertNotNull("can't find file " + filename, is);
		return is;
	}

	@Test
	@Ignore("wip")
	public void itTest() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.newFolder().toPath();

		Path hdtPath = root.resolve("test.hdt");
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
				HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, hdtPath, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
				root.resolve("gd"), HDTOptionsKeys.DICTIONARY_TYPE_KEY,
				HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG, HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK, HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
				// all indexes
				HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, Arrays.stream(TripleComponentOrder.values())
						.map(TripleComponentOrder::name).collect(Collectors.joining(",")));
		ProgressListener listener = ProgressListener.ignore();
		String ns = "http://example.org/#";
		try (InputStream is = getStream("/merge_ds.ttl");
				HDT hdt = HDTManager.generateHDT(is, ns, RDFNotation.TURTLE, spec, listener)) {
			hdt.saveToHDT(hdtPath);
		}

		try (HDT hdt = HDTManager.mapIndexedHDT(hdtPath, spec, listener)) {
			// test index creation
			assertTrue(Files.exists(BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.OPS)));
			assertTrue(Files.exists(BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.POS)));
			assertTrue(Files.exists(BitmapTriplesIndexFile.getIndexPath(hdtPath, TripleComponentOrder.PSO)));

			/*
			 * The query is ~that: SELECT * { ?s ex:relative ?o ?o rdfs:name ?n
			 * ?o ex:id ?id }
			 */

			Dictionary dict = hdt.getDictionary();
			long exRelative = dict.stringToId(ns + "relative", TripleComponentRole.PREDICATE);
			long rdfsName = dict.stringToId("http://www.w3.org/2000/01/rdf-schema#name", TripleComponentRole.PREDICATE);
			long exId = dict.stringToId(ns + "id", TripleComponentRole.PREDICATE);

			TripleID p1 = new TripleID(0, exRelative, 0);
			TripleID p2 = new TripleID(0, rdfsName, 0);
			TripleID p3 = new TripleID(0, exId, 0);

			assertFalse(p1 + " empty", p1.isEmpty());
			assertFalse(p2 + " empty", p2.isEmpty());
			assertFalse(p3 + " empty", p3.isEmpty());

			SuppliableIteratorTripleID it1 = hdt.getTriples().search(p1, TripleComponentOrder.POS.mask);
			SuppliableIteratorTripleID it2 = hdt.getTriples().search(p2, TripleComponentOrder.PSO.mask);
			SuppliableIteratorTripleID it3 = hdt.getTriples().search(p3, TripleComponentOrder.PSO.mask);

			assertSame("invalid order ", TripleComponentOrder.POS, it1.getOrder());
			assertSame("invalid order ", TripleComponentOrder.PSO, it2.getOrder());
			assertSame("invalid order ", TripleComponentOrder.PSO, it3.getOrder());

			HDTMergeJoinIterator it = new HDTMergeJoinIterator(
					List.of(new HDTMergeJoinIterator.MergeIteratorData(it1, TripleComponentRole.OBJECT),
							new HDTMergeJoinIterator.MergeIteratorData(it2, TripleComponentRole.SUBJECT),
							new HDTMergeJoinIterator.MergeIteratorData(it3, TripleComponentRole.SUBJECT)));

			System.out.println(it.hasNext());
			it.forEachRemaining(lst -> System.out
					.println(lst.stream().map(d -> dict.toTripleString(Objects.requireNonNull(d.peek())).toString())
							.collect(Collectors.joining(" - "))));
		}

	}
}
