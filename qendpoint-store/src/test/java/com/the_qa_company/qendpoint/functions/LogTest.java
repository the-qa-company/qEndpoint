package com.the_qa_company.qendpoint.functions;

import com.the_qa_company.qendpoint.store.EndpointStore;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogTest {
	@Test
	public void funcTest() {
		MemoryStore ms = new MemoryStore();
		SailRepository repo = new SailRepository(ms);
		repo.init();
		try {
			Repositories.consumeNoTransaction(repo, conn -> {
				try (TupleQueryResult res = conn.prepareTupleQuery("""
						PREFIX qep: <%s>

						SELECT * {
							VALUES ?number { 10 80 420 10000 20000 1000000 }
							BIND (qep:log(?number) AS ?logNumberDef)
							BIND (qep:log(?number, 10) AS ?logNumber10)
							BIND (qep:log(?number, 16) AS ?logNumber16)
							BIND (qep:log(?number, 2) AS ?logNumber2)
						}
						""".formatted(EndpointStore.BASE_URI)).evaluate()) {
					res.forEach(bind -> {
						double value = ((Literal) bind.getValue("number")).doubleValue();

						assertEquals(((Literal) bind.getValue("logNumberDef")).doubleValue(), Math.log10(value), 0.001);
						assertEquals(((Literal) bind.getValue("logNumber10")).doubleValue(), Math.log10(value), 0.001);
						assertEquals(((Literal) bind.getValue("logNumber16")).doubleValue(),
								Math.log10(value) / Math.log10(16), 0.001);
						assertEquals(((Literal) bind.getValue("logNumber2")).doubleValue(),
								Math.log10(value) / Math.log10(2), 0.001);
					});
				}
			});
		} finally {
			repo.shutDown();
		}
	}

}
