package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.storage.converter.NodeConverter;
import com.the_qa_company.qendpoint.core.util.map.CopyOnWriteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Multi dictionary component, isn't bound to one dictionary
 *
 * @author Antoine Willerval
 */
public class QEPComponent implements Cloneable {
	record SharedElement(long id, DictionarySectionRole role, QEPDataset dataset, String debugMapped) {}

	record PredicateElement(long id, QEPDataset dataset) {}

	private static final Logger logger = LoggerFactory.getLogger(QEPComponent.class);

	Map<Integer, PredicateElement> predicateIds;
	Map<Integer, SharedElement> sharedIds;
	CharSequence value;
	final QEPCore core;

	private QEPComponent(QEPComponent other) {
		this.predicateIds = new HashMap<>(other.predicateIds);
		this.sharedIds = new HashMap<>(other.sharedIds);
		this.core = other.core;
		this.value = other.value;
	}

	QEPComponent(QEPCore core, QEPDataset dataset, DictionarySectionRole role, long id, CharSequence value) {
		this.core = core;
		if (id > 0) {
			switch (role) {
			case PREDICATE -> {
				this.predicateIds = new CopyOnWriteMap<>(Map.of(dataset.uid(), new PredicateElement(id, dataset)));
				this.sharedIds = new CopyOnWriteMap<>(Map.of());
			}
			case SUBJECT, OBJECT, SHARED -> {
				this.predicateIds = new CopyOnWriteMap<>(Map.of());
				this.sharedIds = new CopyOnWriteMap<>(
						Map.of(dataset.uid(), new SharedElement(id, role, dataset, "build")));
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
	 * @return the string value of this component, warning: will search it
	 */
	@SuppressWarnings("resource")
	public CharSequence getString() {
		if (value == null) {
			// searching over the predicates first because the dictionary is
			// probably smaller
			for (PredicateElement pe : predicateIds.values()) {
				if (pe.id != 0) {
					value = pe.dataset().dataset().getDictionary().idToString(pe.id, TripleComponentRole.PREDICATE);
					if (value != null) {
						return value;
					} else {
						logger.warn("value is contained inside a component but isn't linked to a string, id: {}/{}",
								pe.id, TripleComponentRole.PREDICATE);
					}
				}
			}
			for (SharedElement se : sharedIds.values()) {
				if (se.id != 0) {
					value = se.dataset().dataset().getDictionary().idToString(se.id, se.role.asTripleComponentRole());
					if (value != null) {
						return value;
					} else {
						logger.warn("value is contained inside a component but isn't linked to a string, id: {}/{}",
								se.id, se.role);
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

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof QEPComponent other)) {
			return false;
		}
		// check if a predicate id is in both component
		if (!other.predicateIds.isEmpty()) {
			for (var e : predicateIds.entrySet()) {
				if (e.getValue().id == 0) {
					continue; // not defined for this dataset
				}
				PredicateElement pe = other.predicateIds.get(e.getKey());
				if (pe == null || pe.id == 0) {
					continue; // not defined for the other component
				}
				return pe.id == e.getValue().id;
			}
		}
		if (!other.sharedIds.isEmpty()) {
			for (var e : sharedIds.entrySet()) {
				if (e.getValue().id == 0) {
					continue; // not defined for this dataset
				}
				SharedElement se = other.sharedIds.get(e.getKey());
				if (se == null || se.id == 0) {
					continue;
				}
				return se.id == e.getValue().id;
			}
		}
		// we have to check using the strings because no ids are corresponding
		return toString().equals(other.toString());
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
			// we can reach this part if no other dataset are describing this
			// component

			// search by string

			long pid = d2.dataset().getDictionary().stringToId(getString(), TripleComponentRole.PREDICATE);

			// put our find in the map
			if (pid <= 0) {
				predicateIds.put(dataset, new PredicateElement(0, d2));
				return 0;
			} else {
				predicateIds.put(dataset, new PredicateElement(pid, d2));
			}

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

			long nshared = d2.dataset().getDictionary().getNshared();
			for (Map.Entry<Integer, SharedElement> e : sharedIds.entrySet()) {
				if (e.getValue().id() == 0) {
					continue; // undefined
				}

				int originDataset = e.getKey();
				long originId = e.getValue().id();

				NodeConverter converter = core.getConverter(originDataset, dataset,
						e.getValue().role.asTripleComponentRole());
				long mapValue = converter.mapValue(originId);
				long idOfMapped = QEPMap.getIdOfMapped(mapValue, nshared);

				if (idOfMapped <= nshared) {
					// shared or empty element
					if (idOfMapped <= 0) {
						sharedIds.put(dataset, new SharedElement(0, DictionarySectionRole.SHARED, d2, "mapped shared"));
					} else {
						sharedIds.put(dataset,
								new SharedElement(idOfMapped, DictionarySectionRole.SHARED, d2, "mapped shared"));
					}
				} else {
					TripleComponentRole roleOfMapped = QEPMap.getRoleOfMapped(mapValue);
					sharedIds.put(dataset,
							new SharedElement(idOfMapped, roleOfMapped.asDictionarySectionRole(), d2, "mapped role"));
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

			if (id <= 0) {
				// not in the same section, we search on the other side to know
				// if we should put 0 or an ID
				TripleComponentRole otherRole = switch (role) {
				case OBJECT -> TripleComponentRole.SUBJECT;
				case SUBJECT -> TripleComponentRole.OBJECT;
				default -> throw new AssertionError();
				};
				id = d2.dataset().getDictionary().stringToId(seq, otherRole);
				if (id <= nshared) {
					assert id <= 0 : "found shared id";
					sharedIds.put(dataset, new SharedElement(0, DictionarySectionRole.SHARED, d2, "direct 2 shared"));
				} else {
					sharedIds.put(dataset,
							new SharedElement(id, otherRole.asDictionarySectionRole(), d2, "direct 2 role"));
				}
				return 0; // not the same role
			}

			if (id <= nshared) {
				sharedIds.put(dataset, new SharedElement(id, DictionarySectionRole.SHARED, d2, "direct 1 shared"));
			} else {
				sharedIds.put(dataset, new SharedElement(id, role.asDictionarySectionRole(), d2, "direct 1 role"));
			}

			return id;
		}
		default -> throw new AssertionError("unknown triple role: " + role);
		}
	}

	/**
	 * @return dump the component information, warning: will call
	 *         {@link #getString()}
	 */
	public String dumpBinding() {
		StringBuilder bld = new StringBuilder(this.toString());

		bld.append("\npredicateIds: ");

		if (predicateIds.isEmpty()) {
			bld.append("NONE");
		}

		predicateIds.forEach((id, map) -> bld
				.append(String.format("\n- D[%s(%d)] => %X", map.dataset.id(), map.dataset.uid(), map.id())));

		bld.append("\nsharedIds: ");

		if (sharedIds.isEmpty()) {
			bld.append("NONE");
		}

		sharedIds.forEach((id, map) -> bld.append(
				String.format("\n- D[%s(%d)/%s] => %X", map.dataset.id(), map.dataset.uid(), map.role, map.id())));

		bld.append("\n");

		return bld.toString();
	}

	@Override
	public QEPComponent clone() {
		try {
			QEPComponent clone = (QEPComponent) super.clone();
			clone.predicateIds = new HashMap<>(predicateIds);
			clone.sharedIds = new HashMap<>(sharedIds);
			return clone;
		} catch (CloneNotSupportedException e) {
			return new QEPComponent(this);
		}
	}
}
