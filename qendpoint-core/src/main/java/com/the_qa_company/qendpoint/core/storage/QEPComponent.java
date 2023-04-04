package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.storage.converter.NodeConverter;
import com.the_qa_company.qendpoint.core.util.map.CopyOnWriteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Multi dictionary component, isn't bound to one dictionary
 *
 * @author Antoine Willerval
 */
public class QEPComponent {
	private record SharedElement(long id, DictionarySectionRole role, QEPDataset dataset) {
	}

	private record PredicateElement(long id, QEPDataset dataset) {
	}

	private static final Logger logger = LoggerFactory.getLogger(QEPComponent.class);

	private final Map<Integer, PredicateElement> predicateIds;
	private final Map<Integer, SharedElement> sharedIds;
	private final QEPCore core;
	private CharSequence value;

	QEPComponent(QEPCore core, QEPDataset dataset, DictionarySectionRole role, long id, CharSequence value) {
		this.core = core;
		if (id != 0) {
			switch (role) {
				case PREDICATE -> {
					this.predicateIds = new CopyOnWriteMap<>(Map.of(dataset.uid(), new PredicateElement(id, dataset)));
					this.sharedIds = new CopyOnWriteMap<>(Map.of());
				}
				case SUBJECT, OBJECT -> {
					this.predicateIds = new CopyOnWriteMap<>(Map.of());
					this.sharedIds = new CopyOnWriteMap<>(Map.of(dataset.uid(), new SharedElement(id, role, dataset)));
				}
				default -> throw new AssertionError("unknown triple role: " + role);
			}
		} else if (value != null) {
			this.predicateIds = new CopyOnWriteMap<>(Map.of());
			this.sharedIds = new CopyOnWriteMap<>(Map.of());
			this.value = value;
		} else {
			throw new IllegalArgumentException("the id and the value can't be both null");
		}
	}

	/**
	 * @return the string value of this component
	 */
	@SuppressWarnings("resource")
	public CharSequence getString() {
		if (value == null) {
			// searching over the predicates first because the dictionary is probably smaller
			for (PredicateElement pe : predicateIds.values()) {
				if (pe.id != 0) {
					value = pe.dataset().dataset().getDictionary().idToString(pe.id, TripleComponentRole.PREDICATE);
					if (value != null) {
						break;
					} else {
						logger.warn("value is contained inside a component but isn't linked to a string, id: {}/{}", pe.id, TripleComponentRole.PREDICATE);
					}
				}
			}
			for (SharedElement se : sharedIds.values()) {
				if (se.id != 0) {
					value = se.dataset().dataset().getDictionary().idToString(se.id, se.role.asTripleComponentRole());
					if (value != null) {
						break;
					} else {
						logger.warn("value is contained inside a component but isn't linked to a string, id: {}/{}", se.id, se.role);
					}
				}
			}
			throw new AssertionError("Badly config qep component");
		}
		return value;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(toString());
	}

	@Override
	public String toString() {
		return Objects.requireNonNullElse(getString(), "undefined").toString();
	}

	/**
	 * get the id in a particular dataset for a role
	 *
	 * @param dataset dataset uid
	 * @param role    role
	 * @return id in the dataset, or 0 if it doesn't exist in the dataset
	 * @throws QEPCoreException         converter issue
	 * @throws IllegalArgumentException if the dataset doesn't exist
	 */
	public long getId(int dataset, TripleComponentRole role) throws QEPCoreException {
		switch (role) {
			case PREDICATE -> {
				PredicateElement id = predicateIds.get(dataset);
				if (id != null) {
					return id.id();
				}
				QEPDataset d2 = core.getDatasetByUid(dataset);

				if (d2 == null) {
					throw new IllegalArgumentException("searching over an unknown dataset: uid:" + dataset);
				}

				for (Map.Entry<Integer, PredicateElement> e : predicateIds.entrySet()) {
					if (e.getValue().id() == 0) {
						continue; // undefined
					}

					int originDataset = e.getKey();
					long originId = e.getValue().id();

					NodeConverter converter = core.getConverter(originDataset, dataset, TripleComponentRole.PREDICATE);
					long mapValue = converter.mapValue(originId);
					predicateIds.put(dataset, new PredicateElement(mapValue, d2));
					return mapValue;
				}
				// we can reach this part if no other dataset are describing this component

				// search by string

				long pid = d2.dataset().getDictionary().stringToId(getString(), TripleComponentRole.PREDICATE);

				// put our find in the map
				predicateIds.put(dataset, new PredicateElement(pid, d2));

				return pid;
			}
			case SUBJECT, OBJECT -> {
				SharedElement se = sharedIds.get(dataset);

				if (se != null) {
					if (se.role == DictionarySectionRole.SHARED || se.role == role.asDictionarySectionRole()) {
						// same section
						return se.id;
					}
					// not the same section, so not shared, we can put 0
					return 0;
				}
				QEPDataset d2 = core.getDatasetByUid(dataset);

				if (d2 == null) {
					throw new IllegalArgumentException("searching over an unknown dataset: uid:" + dataset);
				}
				// we need to find it

				for (Map.Entry<Integer, SharedElement> e : sharedIds.entrySet()) {
					if (e.getValue().id() == 0) {
						continue; // undefined
					}

					int originDataset = e.getKey();
					long originId = e.getValue().id();

					NodeConverter converter = core.getConverter(originDataset, dataset, e.getValue().role.asTripleComponentRole());
					long mapValue = converter.mapValue(originId);
					long idOfMapped = QEPMap.getIdOfMapped(mapValue);

					if (idOfMapped <= d2.dataset().getDictionary().getNshared()) {
						// shared or empty element
						sharedIds.put(dataset, new SharedElement(idOfMapped, DictionarySectionRole.SHARED, d2));
					} else {
						TripleComponentRole roleOfMapped = QEPMap.getRoleOfMapped(mapValue);
						sharedIds.put(dataset, new SharedElement(
								idOfMapped,
								roleOfMapped.asDictionarySectionRole(),
								d2
						));
						if (role != roleOfMapped) {
							// not in the same section
							return 0;
						}
					}

					return idOfMapped;
				}

				// can't find it, we need to use the string
				CharSequence seq = getString();

				long id = d2.dataset().getDictionary().stringToId(seq, role);

				if (id <= d2.dataset().getDictionary().getNshared()) {
					sharedIds.put(dataset, new SharedElement(id, DictionarySectionRole.SHARED, d2));
				} else {
					sharedIds.put(dataset, new SharedElement(id, role.asDictionarySectionRole(), d2));
				}

				// can't find the id
				return 0;
			}
			default -> throw new AssertionError("unknown triple role: " + role);
		}
	}
}
