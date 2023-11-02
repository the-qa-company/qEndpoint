package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BitmapTriplesOrderTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void orderTest() throws IOException, ParserException {
		Path root = tempDir.newFolder().toPath();

		try {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10_000, 52).withMaxElementSplit(50).withMaxLiteralSize(20);

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.BITMAPTRIPLES_INDEX_NO_FOQ, true,
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, Arrays.stream(TripleComponentOrder.values())
							.map(Object::toString).collect(Collectors.joining(",")));

			Path hdtFile = root.resolve("file.hdt");
			supplier.createAndSaveFakeHDT(spec, hdtFile);

			try (HDT hdt = HDTManager.mapIndexedHDT(hdtFile, spec, ProgressListener.ignore())) {

				// check index creations
				for (TripleComponentOrder order : TripleComponentOrder.values()) {
					if (order == TripleComponentOrder.Unknown || order == TripleComponentOrder.SPO) {
						// default or unknown
						continue;
					}

					Path path = BitmapTriplesIndexFile.getIndexPath(hdtFile, order);
					assertTrue(path + " doesn't exist! order " + order, Files.exists(path));
				}

				// all triples available?
				Set<TripleID> dso = new HashSet<>();

				IteratorTripleID it = hdt.getTriples().searchAll(TripleComponentOrder.SPO.mask);

				assertEquals(TripleComponentOrder.SPO, it.getOrder());
				while (it.hasNext()) {
					TripleID tid = it.next().clone();
					if (!dso.add(tid)) {
						fail("tid " + tid + " was read twice, dso: " + dso);
					}
				}

				assertEquals(hdt.getTriples().getNumberOfElements(), dso.size());
				for (TripleComponentOrder order : TripleComponentOrder.values()) {
					if (order == TripleComponentOrder.Unknown) {
						continue;
					}
					Set<TripleID> ds = new HashSet<>(dso);

					IteratorTripleID it2 = hdt.getTriples().searchAll(order.mask);
					assertEquals(order, it2.getOrder());
					while (it2.hasNext()) {
						TripleID tid = it2.next().clone();
						if (!ds.remove(tid)) {
							fail("tid " + tid + " can't be find, previously here: " + dso.contains(tid));
						}
					}

					assertTrue("ds not empty, " + ds.size() + " elem remaining", ds.isEmpty());
				}
			}

		} finally {
			PathUtils.deleteDirectory(root);
		}
	}
}
