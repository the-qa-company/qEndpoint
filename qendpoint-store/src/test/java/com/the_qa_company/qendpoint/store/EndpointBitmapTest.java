package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleString;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.utils.BitArrayDisk;
import com.the_qa_company.qendpoint.utils.RDFStreamUtils;
import com.the_qa_company.qendpoint.utils.iterators.IteratorToIteration;
import org.apache.commons.io.file.PathUtils;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EndpointBitmapTest {
	public static void assertHDTEquals(Path hdt1Path, Path hdt2Path) throws IOException, NotFoundException {
		try (HDT hdt1 = HDTManager.mapHDT(hdt1Path); HDT hdt2 = HDTManager.mapHDT(hdt2Path)) {
			IteratorTripleString it1 = hdt1.search("", "", "");
			IteratorTripleString it2 = hdt2.search("", "", "");

			while (it1.hasNext()) {
				assertTrue(it2.hasNext());
				TripleString t1 = it1.next();
				TripleString t2 = it2.next();
				assertEquals(t1, t2);
			}
			assertFalse(it2.hasNext());
		}
	}

	public static HDT createHDTFromSail(Sail sail, HDTOptions spec) throws ParserException, IOException {
		try (SailConnection conn = sail.getConnection();
				IteratorToIteration<? extends Statement, SailException> it = new IteratorToIteration<>(
						conn.getStatements(null, null, null, false))) {
			return HDTManager.generateHDT(it.map(t -> {
				String s = t.getSubject().toString();
				String p = t.getPredicate().toString();
				String o;
				Value valueObject = t.getObject();
				if (!valueObject.isLiteral()
						|| !CoreDatatype.XSD.STRING.equals(((Literal) valueObject).getCoreDatatype())) {
					o = valueObject.toString();
				} else {
					o = '"' + valueObject.stringValue() + '"';
				}
				return new TripleString(s, p, o);
			}).asIterator(), "http://example.org/#", spec, ProgressListener.ignore());
		}
	}

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void reinitSimpleTest() throws IOException, ParserException, NotFoundException {
		Path root = tempDir.getRoot().toPath();
		Path tempHDT = root.resolve("hdttemp.hdt");

		EndpointFiles files = new EndpointFiles(root);

		// hdt containing all the triples
		Path hdt = root.resolve("whdt.hdt");

		try {
			LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier
					.createSupplierWithMaxTriples(10_000, 54).withMaxElementSplit(50).withMaxLiteralSize(20);

			HDTOptions spec = HDTOptions.of(HDTOptionsKeys.DICTIONARY_TYPE_KEY,
					HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS, HDTOptionsKeys.LOADER_TYPE_KEY,
					HDTOptionsKeys.LOADER_TYPE_VALUE_DISK, HDTOptionsKeys.LOADER_DISK_LOCATION_KEY,
					root.resolve("wip"));
			supplier.createAndSaveFakeHDT(spec, hdt);
			// clone the start HDT
			Path epHdtIndex = Path.of(files.getHDTIndex());
			Files.createDirectories(epHdtIndex.getParent());
			Files.copy(hdt, epHdtIndex);

			{
				// we create an endpoint with only the HDT file
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
						thdt.saveToHDT(tempHDT, ProgressListener.ignore());
					}
				} finally {
					endpointStore.shutDown();
				}
			}

			// basic test to prove the 2 HDTs are the same
			assertHDTEquals(hdt, tempHDT);
			Files.delete(tempHDT);

			{
				// we reinit the ep to see if we already have the same HDT
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
						thdt.saveToHDT(tempHDT, ProgressListener.ignore());
					}
				} finally {
					endpointStore.shutDown();
				}
			}

			// basic test to prove the 2 HDTs are the same
			assertHDTEquals(hdt, tempHDT);

			Path add2 = root.resolve("add2.hdt");
			supplier.createAndSaveFakeHDT(spec, add2);

			try (HDT catHDT = HDTManager.catHDTPath(List.of(add2, hdt), spec, ProgressListener.ignore())) {
				catHDT.saveToHDT(hdt, ProgressListener.ignore());
			}

			{
				// we start the ep to add the new triples
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (NotifyingSailConnection conn = endpointStore.getConnection();
							HDT hdt2 = HDTManager.mapHDT(add2)) {
						conn.begin();
						// we add all the new triples to the store
						hdt2.search("", "", "").forEachRemaining(s -> {
							Statement stmt = RDFStreamUtils.convertStatement(endpointStore.getValueFactory(), s);
							conn.addStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
						});
						conn.commit();

						try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
							thdt.saveToHDT(tempHDT, ProgressListener.ignore());
						}
					}
				} finally {
					endpointStore.shutDown();
				}
			}

			// basic test to prove the 2 HDTs are the same
			assertHDTEquals(hdt, tempHDT);
			Files.delete(tempHDT);

			{
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
						thdt.saveToHDT(tempHDT, ProgressListener.ignore());
					}
				} finally {
					endpointStore.shutDown();
				}
			}
			assertHDTEquals(hdt, tempHDT);
			Files.delete(tempHDT);

			ModifiableBitmap deleteBitmap;
			try (HDT hdtData = HDTManager.mapHDT(hdt)) {
				long count = hdtData.getTriples().getNumberOfElements();

				assertTrue("test issue", count < Integer.MAX_VALUE);
				deleteBitmap = Bitmap64Big.memory(count + 1);

				Random rnd = new Random(76);
				long subject = hdtData.getDictionary().getNsubjects();
				System.out.println("subjects" + subject);
				assertTrue(subject > 25);

				for (long i = 0; i < subject / 25; i++) {
					IteratorTripleID it = hdtData.getTriples().search(new TripleID(rnd.nextInt((int) subject), 0, 0));
					while (it.hasNext()) {
						it.next();
						deleteBitmap.set(it.getLastTriplePosition(), true);
					}
				}
			}

			{
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (NotifyingSailConnection conn = endpointStore.getConnection();
							HDT hdtData = HDTManager.mapHDT(hdt)) {
						conn.begin();

						// we add all the new triples to the store
						IteratorTripleString it = hdtData.search("", "", "");
						while (it.hasNext()) {
							TripleString triple = it.next();

							if (!deleteBitmap.access(it.getLastTriplePosition())) {
								// ignore this triple because we won't delete it
								continue;
							}

							Statement stmt = RDFStreamUtils.convertStatement(endpointStore.getValueFactory(), triple);
							conn.removeStatements(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
						}
						conn.commit();

						try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
							thdt.saveToHDT(tempHDT, ProgressListener.ignore());
						}
					}
				} finally {
					endpointStore.shutDown();
				}
			}

			// we delete from our HDT the delete triples
			Path diffCatHDT = root.resolve("diffcathdt.hdt");
			Path preDiffCatHDT = root.resolve("prediffcathdt.hdt");
			try (HDT newHDT = HDTManager.diffBitCatHDTPath(List.of(hdt), List.of(deleteBitmap), spec,
					ProgressListener.ignore())) {
				newHDT.saveToHDT(diffCatHDT, ProgressListener.ignore());
			}
			Files.move(hdt, preDiffCatHDT);
			Files.move(diffCatHDT, hdt);

			assertHDTEquals(hdt, tempHDT);
			Files.delete(tempHDT);

			{
				// we reinit the store with the deleted triples to see if it's
				// still fine
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
						thdt.saveToHDT(tempHDT, ProgressListener.ignore());
					}
				} finally {
					endpointStore.shutDown();
				}
			}

			assertHDTEquals(hdt, tempHDT);
			Files.delete(tempHDT);

			{
				// we start the ep to add the new triples
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (NotifyingSailConnection conn = endpointStore.getConnection();
							HDT preDiffCatHDTData = HDTManager.mapHDT(preDiffCatHDT)) {
						conn.begin();
						// we add back all the new triples to the store
						IteratorTripleString it = preDiffCatHDTData.search("", "", "");
						while (it.hasNext()) {
							TripleString ts = it.next();
							if (deleteBitmap.access(it.getLastTriplePosition())) {
								Statement stmt = RDFStreamUtils.convertStatement(endpointStore.getValueFactory(), ts);
								conn.addStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject());
							}
						}
						conn.commit();

						try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
							thdt.saveToHDT(tempHDT, ProgressListener.ignore());
						}
					}
				} finally {
					endpointStore.shutDown();
				}
			}
			// we reput the old hdt into our dataset hdt
			Files.delete(hdt);
			Files.move(preDiffCatHDT, hdt);

			assertHDTEquals(hdt, tempHDT);
			Files.delete(tempHDT);

			{
				EndpointStore endpointStore = new EndpointStore(files);

				try {
					try (HDT thdt = createHDTFromSail(endpointStore, spec)) {
						thdt.saveToHDT(tempHDT, ProgressListener.ignore());
					}
				} finally {
					endpointStore.shutDown();
				}
			}

			assertHDTEquals(hdt, tempHDT);
			Files.delete(tempHDT);

		} finally {
			PathUtils.deleteDirectory(root);
		}
	}
}
