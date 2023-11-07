package com.the_qa_company.qendpoint.core.enums;

import java.util.HashMap;

public enum WikidataChangesFlavor {
	/**
	 * Excludes descriptions of entities referred to in the data
	 */
	DUMP("dump", true, "Excludes descriptions of entities referred to in the data.", (byte) 0x63),
	/**
	 * Provides only truthy statements, along with sitelinks and version
	 * information.
	 */
	SIMPLE("simple", true, "Provides only truthy statements, along with sitelinks and version information.",
			(byte) 0x64),
	/**
	 * An argument of "full" returns all data.
	 */
	FULL("full", false, "An argument of \"full\" returns all data.", (byte) 0x65);

	private static final HashMap<Byte, WikidataChangesFlavor> FLAVOR_HASH_MAP = new HashMap<>();

	static {
		for (WikidataChangesFlavor fl : values()) {
			FLAVOR_HASH_MAP.put(fl.id, fl);
		}
	}

	public final String title;
	public final boolean shouldSpecify;
	public final String description;
	public final byte id;

	WikidataChangesFlavor(String title, boolean shouldSpecify, String description, byte id) {
		this.title = title;
		this.shouldSpecify = shouldSpecify;
		this.description = description;
		this.id = id;
	}

	/**
	 * @return the default flavor
	 */
	public static WikidataChangesFlavor getDefaultFlavor() {
		return FULL;
	}

	/**
	 * get a flavor from its id
	 *
	 * @param id id
	 * @return flavor or null
	 */
	public static WikidataChangesFlavor getFromId(byte id) {
		return FLAVOR_HASH_MAP.get(id);
	}

}
