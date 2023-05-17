package com.the_qa_company.qendpoint.core.storage;

import java.util.Objects;

/**
 * UID used for linking {@link QEPDataset} with {@link QEPMap} in
 * {@link QEPCore}
 *
 * @author Antoine Willerval
 */
public class Uid {

	/**
	 * create a sorted uid
	 *
	 * @param uid1 uid1
	 * @param uid2 uid2
	 * @return Uid with sorted components uid1<uid2
	 */
	public static Uid of(int uid1, int uid2) {
		if (uid1 < uid2) {
			return new Uid(uid1, uid2);
		}

		return new Uid(uid2, uid1);
	}

	private final int uid1;
	private final int uid2;

	private Uid(int uid1, int uid2) {
		if (uid1 == uid2) {
			throw new IllegalArgumentException("Can't create UID with the same ids");
		}
		this.uid1 = uid1;
		this.uid2 = uid2;
	}

	/**
	 * @return small uid
	 */
	public int uid1() {
		return uid1;
	}

	/**
	 * @return big uid
	 */
	public int uid2() {
		return uid2;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Uid uid = (Uid) o;
		return uid1 == uid.uid1 && uid2 == uid.uid2;
	}

	@Override
	public int hashCode() {
		return Objects.hash(uid1, uid2);
	}

	@Override
	public String toString() {
		return "Uid{" + "uid1=" + uid1 + ", uid2=" + uid2 + '}';
	}
}
