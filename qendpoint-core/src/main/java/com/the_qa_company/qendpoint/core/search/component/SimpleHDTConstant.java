package com.the_qa_company.qendpoint.core.search.component;

import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.util.StringUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;

import java.util.Objects;

public class SimpleHDTConstant implements HDTConstant {
	/**
	 * Special char sequence, won't be resolved with any RDF object
	 */
	public static final CharSequence UNDEFINED = ByteString.of("UNDEFINED");
	private final HDT hdt;
	private long shid;
	private DictionarySectionRole sharedLocation;
	private long predicate;
	private CharSequence value;

	private SimpleHDTConstant(SimpleHDTConstant other) {
		this.hdt = other.hdt;
		this.sharedLocation = other.sharedLocation;
		this.shid = other.shid;
		this.predicate = other.predicate;
		this.value = StringUtil.copy(other.value);
	}

	public SimpleHDTConstant(HDT hdt, long id, DictionarySectionRole location) {
		this.hdt = hdt;
		switch (Objects.requireNonNull(location, "location can't be null!")) {
		case SUBJECT:
		case OBJECT:
			shid = id;
			if (id > hdt.getDictionary().getShared().getNumberOfElements()) {
				sharedLocation = location;
			} else {
				sharedLocation = DictionarySectionRole.SHARED;
			}
			break;
		case SHARED:
			shid = id;
			sharedLocation = location;
			break;
		case PREDICATE:
			predicate = id;
			break;
		}
	}

	public SimpleHDTConstant(HDT hdt, CharSequence value) {
		this.hdt = Objects.requireNonNull(hdt, "hdt can't be null!");
		this.value = Objects.requireNonNullElse(value, "");
	}

	@Override
	public CharSequence getValue() {
		if (value == null) {
			// at least shid or predicate is set
			if (predicate > 0) {
				value = hdt.getDictionary().getPredicates().extract(predicate);
			} else {
				value = hdt.getDictionary().idToString(shid, sharedLocation.asTripleComponentRole());
			}
		}
		if (value == null) {
			value = UNDEFINED;
		}
		return value;
	}

	/**
	 * set the value of this constant
	 *
	 * @param value value
	 */
	public void setValue(CharSequence value) {
		this.value = Objects.requireNonNullElse(value, "");
		predicate = 0;
		shid = 0;
		sharedLocation = null;
	}

	public void setId(DictionarySectionRole role, long id) {
		this.value = null;
		Objects.requireNonNull(role);
		if (role == DictionarySectionRole.PREDICATE) {
			if (this.predicate != id) {
				predicate = id;
				this.shid = 0;
				sharedLocation = null;
			}
		} else {
			if (this.shid != id || this.sharedLocation != role) {
				predicate = 0;
				this.shid = id;
				if (id > hdt.getDictionary().getShared().getNumberOfElements()) {
					sharedLocation = role;
				} else {
					sharedLocation = DictionarySectionRole.SHARED;
				}
			}
		}
	}

	@Override
	public long getId(DictionarySectionRole role) {
		if (value == UNDEFINED) {
			return -1;
		}
		if (role == DictionarySectionRole.PREDICATE) {
			if (predicate == 0) {
				CharSequence value = getValue();
				if (value.length() == 0) {
					return 0; // wildcard
				}
				predicate = hdt.getDictionary().getPredicates().locate(value);
			}
			return predicate;
		}
		if (sharedLocation != null) {
			if (sharedLocation == DictionarySectionRole.SHARED || sharedLocation == role) {
				return shid;
			}
			return -1; // not in this section
		}

		TripleComponentRole tripleRole = role.asTripleComponentRole();
		assert tripleRole != null;
		CharSequence value = getValue();
		if (value.length() == 0) {
			return 0; // wildcard
		}
		long id = hdt.getDictionary().stringToId(value, tripleRole);

		if (id == -1) {
			// search on the other side for next time, TODO: storing the fact
			// that this is not a "role"
			// would be better instead of computing into the other dictionary.
			long oid;
			TripleComponentRole tripleRoleNew;
			if (tripleRole == TripleComponentRole.SUBJECT) {
				tripleRoleNew = TripleComponentRole.OBJECT;
				oid = hdt.getDictionary().stringToId(value, tripleRoleNew);
			} else {
				tripleRoleNew = TripleComponentRole.SUBJECT;
				oid = hdt.getDictionary().stringToId(value, tripleRoleNew);
			}

			shid = oid;
			sharedLocation = tripleRoleNew.asDictionarySectionRole();
			return -1;
		}

		// set the value and store the location
		shid = id;

		if (id <= hdt.getDictionary().getShared().getNumberOfElements()) {
			sharedLocation = DictionarySectionRole.SHARED;
		} else {
			// use the base tripleRole instead of role to remove the shared
			sharedLocation = tripleRole.asDictionarySectionRole();
		}

		return shid;
	}

	@Override
	public String stringValue() {
		return getValue().toString();
	}

	@Override
	public String toString() {
		String s = stringValue();
		if (s.isEmpty()) {
			return "[]";
		}
		if (s.charAt(0) == '_' || s.charAt(0) == '"') {
			return s;
		}
		return '<' + s + '>';
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof HDTConstant)) {
			return false;
		}

		if (obj instanceof SimpleHDTConstant) {
			SimpleHDTConstant c = (SimpleHDTConstant) obj;
			// both predicate loaded
			if (predicate != 0 && predicate == c.predicate) {
				return true;
			}

			// both shared loaded
			if (shid != 0 && shid == c.shid && sharedLocation == c.sharedLocation) {
				return true;
			}
		}
		// we load the strings because we can't compare with IDs
		return CharSequenceComparator.getInstance().compare(getValue(), ((HDTConstant) obj).getValue()) == 0;
	}

	@Override
	public HDTConstant copy() {
		return new SimpleHDTConstant(this);
	}

	@Override
	public int hashCode() {
		return Objects.hash(stringValue());
	}
}
