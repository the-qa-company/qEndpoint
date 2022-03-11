package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.utils.sail.filter.TypeSailFilter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class MultiTypeFilteringSail extends SailWrapper {
	private Map<Resource, Value> lastTypeBuffer;
	private final Sail tripleSourceSail;

	public MultiTypeFilteringSail(Sail tripleSourceSail, IRI predicate,
								  TypedSail... types) {
		super();
		this.tripleSourceSail = tripleSourceSail;

		Sail baseSail = tripleSourceSail;
		for (TypedSail type: types) {
			baseSail = new FilteringSail(type.sail, baseSail, type.sailConsumer,
					(connection) -> new TypeSailFilter(
							lastTypeBuffer, connection, predicate, type.type
					)
			);
		}

		setBaseSail(baseSail);
	}

	public Sail getTripleSourceSail() {
		return tripleSourceSail;
	}

	@Override
	public synchronized SailConnection getConnection() throws SailException {
		lastTypeBuffer = new HashMap<>();
		return super.getConnection();
	}

	public static class TypedSail {
		private final Sail sail;
		private final Value type;
		private final Consumer<Sail> sailConsumer;

		public TypedSail(Sail sail, Value type, Consumer<Sail> sailConsumer) {
			this.sail = sail;
			this.type = type;
			this.sailConsumer = sailConsumer;
		}

		public Sail getSail() {
			return sail;
		}

		public Value getType() {
			return type;
		}
	}
}
