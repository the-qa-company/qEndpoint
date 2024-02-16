package com.the_qa_company.qendpoint.store.compliance;

import com.the_qa_company.qendpoint.store.Utility;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.testsuite.query.parser.sparql.manifest.SPARQL11QueryComplianceTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class CTSPARQL11QueryComplianceTest extends SPARQL11QueryComplianceTest {
	public static boolean PRINT = true;
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	protected CTSPARQL11QueryComplianceTest(String... testToIgnore) {
		this.setIgnoredTests(List.of(testToIgnore));
	}

	@Override
	protected final Repository newRepository() throws Exception {
		Sail sail = newSail();
		if (PRINT) {
			return Utility.convertToDumpRepository(new SailRepository(Utility.convertToDumpSail(sail)));
		}
		return new SailRepository(sail);
	}
	protected abstract Sail newSail() throws Exception;
}