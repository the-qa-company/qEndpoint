package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.utils.sail.filter.TypeSailFilter;
import com.the_qa_company.q_endpoint.utils.sail.linked.LinkedSail;
import com.the_qa_company.q_endpoint.utils.sail.linked.SimpleLinkedSail;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * a wrapper to filter multiple sail with a {@link com.the_qa_company.q_endpoint.utils.sail.filter.TypeSailFilter}.
 *
 * @author Antoine Willerval
 */
public class MultiTypeFilteringSail extends NotifyingSailWrapper implements LinkedSail<MultiTypeFilteringSail> {
	private Map<Resource, Value> lastTypeBuffer;
	private NotifyingSail tripleSourceSail;
	private final List<TypedSail> types;
	private final IRI predicate;

	/**
	 * create a sail to filter multiple sails by their type
	 *
	 * @param tripleSourceSail the source to put bellow all the typed sail
	 * @param predicate        the predicate to define the type of a subject
	 * @param types            the typed sail to redirect
	 */
	public MultiTypeFilteringSail(NotifyingSail tripleSourceSail, IRI predicate, TypedSail... types) {
		this(tripleSourceSail, predicate, Arrays.asList(types));
	}
	/**
	 * create a sail to filter multiple sails by their type
	 *
	 * @param tripleSourceSail the source to put bellow all the typed sail
	 * @param predicate        the predicate to define the type of a subject
	 * @param types            the typed sail to redirect
	 */
	public MultiTypeFilteringSail(NotifyingSail tripleSourceSail, IRI predicate, List<TypedSail> types) {
		super();
		this.types = types;
		this.predicate = predicate;
		if (tripleSourceSail != null) {
			setTripleSourceSail(tripleSourceSail);
		}
	}

	/**
	 * create a sail to filter multiple sails by their type without triple source, use
	 * {@link #setBaseSail(org.eclipse.rdf4j.sail.Sail)} or
	 * {@link #setTripleSourceSail(org.eclipse.rdf4j.sail.NotifyingSail)} to set the source
	 *
	 * @param predicate        the predicate to define the type of a subject
	 * @param types            the typed sail to redirect
	 */
	public MultiTypeFilteringSail(IRI predicate, TypedSail... types) {
		this(predicate, Arrays.asList(types));
	}
	/**
	 * create a sail to filter multiple sails by their type without triple source, use
	 * {@link #setBaseSail(org.eclipse.rdf4j.sail.Sail)} or
	 * {@link #setTripleSourceSail(org.eclipse.rdf4j.sail.NotifyingSail)} to set the source
	 *
	 * @param predicate        the predicate to define the type of a subject
	 * @param types            the typed sail to redirect
	 */
	public MultiTypeFilteringSail(IRI predicate, List<TypedSail> types) {
		this(null, predicate, types);
	}

	/**
	 * set the triple source sail and link the filters
	 * @param tripleSourceSail the triple source
	 */
	public void setTripleSourceSail(NotifyingSail tripleSourceSail) {
		this.tripleSourceSail = tripleSourceSail;

		NotifyingSail baseSail = tripleSourceSail;
		// inverse loop to link the last element to the source
		for (int i = types.size() - 1; i >= 0; i--) {
			TypedSail type = types.get(i);
			baseSail = new FilteringSail(type, baseSail,
					(connection) -> new TypeSailFilter(
							lastTypeBuffer, connection, predicate, type.getType()
					)
			);
		}
		super.setBaseSail(baseSail);
	}
	@Override
	public void setBaseSail(Sail tripleSourceSail) {
		setTripleSourceSail((NotifyingSail) tripleSourceSail);
	}

	/**
	 * @return the triple source of this sail
	 */
	public Sail getTripleSourceSail() {
		return tripleSourceSail;
	}

	@Override
	public synchronized NotifyingSailConnection getConnection() throws SailException {
		lastTypeBuffer = new HashMap<>();
		return super.getConnection();
	}

	@Override
	public MultiTypeFilteringSail getSail() {
		return this;
	}

	@Override
	public Consumer<Sail> getSailConsumer() {
		return this::setBaseSail;
	}

	/**
	 * a sail and a type describing it
	 * @author Antoine Willerval
	 */
	public static class TypedSail extends SimpleLinkedSail<NotifyingSail> {
		private final Value type;

		/**
		 * create a typed sail with a sail consumer to define the end
		 *
		 * @param sail the sail to redirect
		 * @param type the type associate with this filtered sail
		 * @param sailConsumer the consumer to define the base sail of the sail
		 */
		public TypedSail(NotifyingSail sail, Value type, Consumer<Sail> sailConsumer) {
			super(sail, sailConsumer);
			this.type = type;
		}
		/**
		 * create a typed sail with a sail consumer to define the end
		 *
		 * @param type the type associate with this filtered sail
		 * @param linkedSails the sails to redirect
		 */
		public TypedSail(LinkedSail<? extends NotifyingSail> linkedSails, Value type) {
			super(linkedSails);
			this.type = type;
		}
		/**
		 * @return the type of the sail
		 */
		public Value getType() {
			return type;
		}
	}
}
