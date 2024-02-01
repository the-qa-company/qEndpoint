package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.compiler.CompiledSail;
import com.the_qa_company.qendpoint.compiler.SparqlRepository;
import com.the_qa_company.qendpoint.core.compact.bitmap.MultiLayerBitmapWrapper;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.model.HDTValue;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class EndpointStoreGraphTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();


	public static String posStr(int pos) {
		return switch (pos) {
			case SimpleIRIHDT.SUBJECT_POS -> "s";
			case SimpleIRIHDT.PREDICATE_POS -> "p";
			case SimpleIRIHDT.OBJECT_POS -> "o";
			case SimpleIRIHDT.SHARED_POS -> "sh";
			case SimpleIRIHDT.GRAPH_POS -> "g";
			default -> "unk" + pos;
		};
	}

	public static String printVal(Value val) {
		if (val instanceof HDTValue hv) {
			return hv + "(" + hv.getHDTId() + "/" + posStr(hv.getHDTPosition()) + ")";
		}
		return val.toString();
	}
	public static void printStmt(Statement stmt) {
		System.out.print(printVal(stmt.getSubject()) + ", " + printVal(stmt.getPredicate()) + ", " + printVal(stmt.getObject()));
		if (stmt.getContext() != null) {
			System.out.println(" [" + printVal(stmt.getContext()) + "]");
		} else {
			System.out.println();
		}
	}

	@Before
	public void setupTest() {
		MergeRunnableStopPoint.debug = true;
	}

	@After
	public void clearTest() {
		MergeRunnableStopPoint.debug = false;
		MergeRunnableStopPoint.unlockAll();
	}

	@Test
	public void graphTest() throws IOException, InterruptedException, NotFoundException {
		File roo = tempDir.getRoot();
		HDTOptions spec = HDTOptions.of(
				// dict
				HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD
				// temp dict
				, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_HASH_QUAD
				// merge join?
				//, EndpointStore.OPTION_QENDPOINT_MERGE_JOIN, false

		);
		SparqlRepository sparqlRepository = CompiledSail.compiler()
				.withEndpointFiles(new EndpointFiles(roo.toPath()))
				.withHDTSpec(spec).compileToSparqlRepository();

		EndpointStore endpointStore = (EndpointStore) ((CompiledSail) sparqlRepository.getRepository().getSail()).getSource();

		ValueFactory vf = sparqlRepository.getRepository().getValueFactory();
		try {
			{
				System.out.println("ADD");
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					conn.begin();
					conn.add(vf.createStatement(
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph")
					));
					conn.add(vf.createStatement(
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa")
					));
					conn.add(vf.createStatement(
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "ss"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa5")
					));
					conn.add(vf.createStatement(
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "ss"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa6")
					));
					conn.commit();
				}
			}
			System.out.println("PRE MERGE");
			{
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					try (RepositoryResult<Statement> st = conn.getStatements(null, null, null, false)) {
						st.stream().forEach(EndpointStoreGraphTest::printStmt);
					}

				}
			}
			System.out.println("MERGE");
			endpointStore.mergeStore();
			MergeRunnable.debugWaitMerge();
			{
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					System.out.println("SEARCH GRAPH");
					try (RepositoryResult<Statement> st = conn.getStatements(null, null, null, false, vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph"))) {
						st.stream().forEach(EndpointStoreGraphTest::printStmt);
					}
					System.out.println("SEARCH NO GRAPH");
					try (RepositoryResult<Statement> st = conn.getStatements(null, null, null, false, (Resource) null)) {
						st.stream().forEach(EndpointStoreGraphTest::printStmt);
					}
					System.out.println("SEARCH BOTH GRAPH");
					try (RepositoryResult<Statement> st = conn.getStatements(null, null, null, false, vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph"), null)) {
						st.stream().forEach(EndpointStoreGraphTest::printStmt);
					}

				}
			}
			{
				System.out.println("REM DEFAULT");
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					conn.begin();
					conn.remove(
					vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
					vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
					(Resource) null);
					conn.commit();
				}
			}
			{
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					try (RepositoryResult<Statement> st = conn.getStatements(null, null, null, false)) {
						st.stream().forEach(EndpointStoreGraphTest::printStmt);
					}

				}
			}
			{
				System.out.println("REM GRAPH");
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					conn.begin();
					conn.remove(vf.createStatement(
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa")
					), vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph"));
					conn.commit();
				}
			}
			{
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					try (RepositoryResult<Statement> st = conn.getStatements(null, null, null, false)) {
						st.stream().forEach(EndpointStoreGraphTest::printStmt);
					}

				}
			}
			System.out.println("deletes:");
			long size = endpointStore.getHdt().getTriples().getNumberOfElements();
			long graphs = endpointStore.getGraphsCount();
			for (int i = 0; i < endpointStore.getDeleteBitMaps().length; i++) {
				TripleComponentOrder order = TripleComponentOrder.values()[i];
				MultiLayerBitmapWrapper.MultiLayerModBitmapWrapper bm = endpointStore.getDeleteBitMaps()[i];
				if (bm == null) {
					continue;
				}

				System.out.println(order);
				for (int g = 0; g < graphs; g++) {
					System.out.print("g" + (g + 1) + ": ");
					for (long t = 0; t < size; t++) {
						System.out.print(bm.access(g, t) ? "1" : "0");
					}
					System.out.println();
				}
			}
			System.out.println("ADD");
			{
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					conn.begin();
					conn.add(vf.createStatement(
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa2"),
							vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph2")
					));
					conn.commit();
				}
			}
			System.out.println("MERGE");
			MergeRunnableStopPoint.STEP2_START.debugLock();
			MergeRunnableStopPoint.STEP2_START.debugLockTest();
			MergeRunnableStopPoint.STEP2_END.debugLock();
			MergeRunnableStopPoint.STEP2_END.debugLockTest();
			endpointStore.mergeStore();

			MergeRunnableStopPoint.STEP2_START.debugWaitForEvent();
			{
				System.out.println("ADD merge");
				{
					try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
						conn.begin();
						conn.add(vf.createStatement(
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa3"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph2")
						));
						conn.commit();
					}
				}
			}
			MergeRunnableStopPoint.STEP2_START.debugUnlockTest();
			MergeRunnableStopPoint.STEP2_END.debugWaitForEvent();
			{
				System.out.println("ADD merge 2");
				{
					try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
						conn.begin();
						conn.add(vf.createStatement(
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa4"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph2")
						));
						conn.commit();
					}
					try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
						conn.begin();
						conn.add(vf.createStatement(
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "o"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa3"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "graph2")
						));
						conn.commit();
					}
					System.out.println("REM 6");
					try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
						conn.begin();
						conn.remove(
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "ss"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "p"),
								vf.createIRI(Utility.EXAMPLE_NAMESPACE + "aa6"),
								(Resource) null);
						conn.commit();
					}
				}
			}
			MergeRunnableStopPoint.STEP2_END.debugUnlockTest();

			MergeRunnable.debugWaitMerge();
			{
				try (SailRepositoryConnection conn = sparqlRepository.getConnection()) {
					try (RepositoryResult<Statement> st = conn.getStatements(null, null, null, false)) {
						st.stream().forEach(EndpointStoreGraphTest::printStmt);
					}
				}
			}
			System.out.println("triples: " + endpointStore.getHdt().getTriples().getNumberOfElements());
			endpointStore.getHdt().searchAll().forEachRemaining(System.out::println);
		} finally {
			sparqlRepository.shutDown();
		}

	}
}
