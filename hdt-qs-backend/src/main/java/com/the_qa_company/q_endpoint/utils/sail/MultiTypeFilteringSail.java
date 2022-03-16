package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.utils.sail.filter.TypeSailFilter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * a wrapper to filter multiple sail with a {@link com.the_qa_company.q_endpoint.utils.sail.filter.TypeSailFilter}.
 *
 * @author Antoine Willerval
 */
public class MultiTypeFilteringSail extends SailWrapper {
	private Map<Resource, Value> lastTypeBuffer;
	private final Sail tripleSourceSail;

	/**
	 * create a sail to filter multiple sails by their type
	 *
	 * @param tripleSourceSail the source to put bellow all the typed sail
	 * @param predicate        the predicate to define the type of a subject
	 * @param types            the typed sail to redirect
	 */
	public MultiTypeFilteringSail(NotifyingSail tripleSourceSail, IRI predicate,
								  TypedSail... types) {
		super();
		this.tripleSourceSail = tripleSourceSail;

		NotifyingSail baseSail = tripleSourceSail;
		// inverse loop to link the last element to the source
		for (int i = types.length - 1; i >= 0; i--) {
			TypedSail type = types[i];
			baseSail = new FilteringSail(type.getSail(), baseSail, type.getSailConsumer(),
					(connection) -> new TypeSailFilter(
							lastTypeBuffer, connection, predicate, type.getType()
					)
			);
		}

		super.setBaseSail(baseSail);
	}

	@Override
	public void setBaseSail(Sail baseSail) {
		throw new IllegalArgumentException("Can't redefine the base sail of a MultiTypeFilteringSail!");
	}

	/**
	 * @return the triple source of this sail
	 */
	public Sail getTripleSourceSail() {
		return tripleSourceSail;
	}

	@Override
	public synchronized SailConnection getConnection() throws SailException {
		lastTypeBuffer = new HashMap<>();
		return super.getConnection();
	}

	/**
	 * a sail and a type describing it
	 * @author Antoine Willerval
	 */
	public static class TypedSail {
		private final NotifyingSail sail;
		private final Value type;
		private final Consumer<Sail> sailConsumer;

		/**
		 * create a typed sail with a sail consumer to define the end
		 *
		 * @param sail the sail to redirect
		 * @param type the type associate with this filtered sail
		 * @param sailConsumer the consumer to define the base sail of the sail
		 */
		public TypedSail(NotifyingSail sail, Value type, Consumer<Sail> sailConsumer) {
			this.sail = sail;
			this.type = type;
			this.sailConsumer = sailConsumer;
		}

		/**
		 * @return the described sail
		 */
		public NotifyingSail getSail() {
			return sail;
		}

		/**
		 * @return the type of the sail
		 */
		public Value getType() {
			return type;
		}

		/**
		 * @return the consumer to set the base sail
		 */
		public Consumer<Sail> getSailConsumer() {
			return sailConsumer;
		}
	}
}
