package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QEPComponentTest {
	@SuppressWarnings("resource")
	public static void assertMapping(QEPComponent component) {
		CharSequence value = component.value;
		QEPDataset origin = null;

		// check known values
		for (QEPComponent.PredicateElement pe : component.predicateIds.values()) {
			QEPDataset dataset = pe.dataset();
			long componentId = pe.id();

			if (componentId == 0) {
				continue;
			}
			CharSequence v = dataset.dataset().getDictionary().idToString(componentId, TripleComponentRole.PREDICATE);

			if (value == null) {
				value = v;
				origin = dataset;
			} else {
				assertEquals("old and new mapping not the same for origin=" + origin + " in " + dataset, value, v);
			}
		}
		for (QEPComponent.SharedElement se : component.sharedIds.values()) {
			QEPDataset dataset = se.dataset();
			long componentId = se.id();
			DictionarySectionRole role = se.role();

			if (componentId == 0) {
				continue;
			}

			CharSequence v = dataset.dataset().getDictionary().idToString(componentId, role.asTripleComponentRole());

			if (value == null) {
				value = v;
				origin = dataset;
			} else {
				assertEquals("old and new mapping not the same for origin=" + origin + " in " + dataset, value, v);
			}
		}

		assert value != null : "value wasn't found";

		// check unset values
		for (QEPComponent.PredicateElement pe : component.predicateIds.values()) {
			QEPDataset dataset = pe.dataset();
			long componentId = pe.id();

			if (componentId != 0) {
				continue;
			}

			long val = dataset.dataset().getDictionary().stringToId(value, TripleComponentRole.PREDICATE);
			assertTrue("(P) value was found for " + value, val <= 0);
		}
		for (QEPComponent.SharedElement se : component.sharedIds.values()) {
			QEPDataset dataset = se.dataset();
			long componentId = se.id();

			if (componentId != 0) {
				continue;
			}

			long valS = dataset.dataset().getDictionary().stringToId(value, TripleComponentRole.SUBJECT);
			assertTrue("(S) value was found for " + value + ": " + valS, valS <= 0);
			long valO = dataset.dataset().getDictionary().stringToId(value, TripleComponentRole.OBJECT);
			assertTrue("(O) value was found for " + value + ": " + valO, valO <= 0);
		}

	}
}
