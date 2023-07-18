package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.Key;
import com.the_qa_company.qendpoint.core.storage.merge.QEPCoreMergeThread;

/**
 * keys usable with {@link HDTOptions#set(String, String)} related with
 * {@link QEPCore}, see
 * {@link com.the_qa_company.qendpoint.core.options.HDTOptionsKeys} for the keys
 * usable with the dataset generation
 *
 * @author Antoine Willerval
 */
public class QEPCoreOptions {
	/**
	 * Key for the threshold before merging a cluster for the
	 * {@link QEPCoreMergeThread}.
	 */
	@Key(type = Key.Type.NUMBER, desc = "QEPCore merge threshold")
	public static final String QEPC_MERGE_THRESHOLD = "qepcore.merge.threshold";
	/**
	 * Key for the merge epsilon between each dataset
	 * {@link QEPCoreMergeThread}.
	 */
	@Key(type = Key.Type.NUMBER, desc = "QEPCore merge epsilon between each dataset")
	public static final String QEPC_MERGE_EPSILON = "qepcore.merge.epsilon";
	/**
	 * Key for map value function for the size of the dataset
	 * {@link QEPCoreMergeThread}.
	 */
	@Key(type = Key.Type.ENUM, desc = "QEPCore map value function for the size of the dataset")
	public static final String QEPC_MERGE_NUMFUNC = "qepcore.merge.numfunc";
	/**
	 * Key to log the merge process with the core progress listener in
	 * {@link QEPCoreMergeThread}.
	 */
	@Key(type = Key.Type.NUMBER, desc = "QEPCore merge epsilon between each dataset")
	public static final String QEPC_MERGE_PROGRESS = "qepcore.merge.progress";
}
