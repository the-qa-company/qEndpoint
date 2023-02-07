package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.WriteLongArrayBuffer;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.impl.CompressFourSectionDictionary;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Map a compress triple file to long array map files
 *
 * @author Antoine Willerval
 */
public class CompressTripleMapper implements CompressFourSectionDictionary.NodeConsumer {
	private static final Logger log = LoggerFactory.getLogger(CompressTripleMapper.class);
	private final WriteLongArrayBuffer subjects;
	private final WriteLongArrayBuffer predicates;
	private final WriteLongArrayBuffer objects;
	private final CloseSuppressPath locationSubjects;
	private final CloseSuppressPath locationPredicates;
	private final CloseSuppressPath locationObjects;
	private long shared = -1;
	private final long tripleCount;

	public CompressTripleMapper(CloseSuppressPath location, long tripleCount, long chunkSize) {
		this.tripleCount = tripleCount;
		locationSubjects = location.resolve("map_subjects");
		locationPredicates = location.resolve("map_predicates");
		locationObjects = location.resolve("map_objects");
		int numbits = BitUtil.log2(tripleCount + 2) + CompressUtil.INDEX_SHIFT;
		int maxElement = (int) Math.min(chunkSize / Long.BYTES / 3, Integer.MAX_VALUE - 5);
		subjects = new WriteLongArrayBuffer(
				new SequenceLog64BigDisk(locationSubjects.toAbsolutePath().toString(), numbits, tripleCount + 2, true),
				tripleCount, maxElement);
		predicates = new WriteLongArrayBuffer(new SequenceLog64BigDisk(locationPredicates.toAbsolutePath().toString(),
				numbits, tripleCount + 2, true), tripleCount, maxElement);
		objects = new WriteLongArrayBuffer(
				new SequenceLog64BigDisk(locationObjects.toAbsolutePath().toString(), numbits, tripleCount + 2, true),
				tripleCount, maxElement);
	}

	/**
	 * delete the map files and the location files
	 */
	public void delete() {
		try {
			IOUtil.closeAll(subjects, predicates, objects);
		} catch (IOException e) {
			log.warn("Can't close triple map array", e);
		}
		try {
			IOUtil.closeAll(locationSubjects, locationPredicates, locationObjects);
		} catch (IOException e) {
			log.warn("Can't delete triple map array files", e);
		}
	}

	@Override
	public void onSubject(long preMapId, long newMapId) {
		assert preMapId > 0;
		assert newMapId >= CompressUtil.getHeaderId(1);
		subjects.set(preMapId, newMapId);
	}

	@Override
	public void onPredicate(long preMapId, long newMapId) {
		assert preMapId > 0;
		assert newMapId >= CompressUtil.getHeaderId(1);
		predicates.set(preMapId, newMapId);
	}

	@Override
	public void onObject(long preMapId, long newMapId) {
		assert preMapId > 0;
		assert newMapId >= CompressUtil.getHeaderId(1);
		objects.set(preMapId, newMapId);
	}

	public void setShared(long shared) {
		this.shared = shared;
		subjects.free();
		predicates.free();
		objects.free();
	}

	private void checkShared() {
		if (this.shared < 0) {
			throw new IllegalArgumentException("Shared not set!");
		}
	}

	/**
	 * extract the map id of a subject
	 *
	 * @param id id
	 * @return new id
	 */
	public long extractSubject(long id) {
		return extract(subjects, id);
	}

	/**
	 * extract the map id of a predicate
	 *
	 * @param id id
	 * @return new id
	 */
	public long extractPredicate(long id) {
		return extract(predicates, id) - shared;
	}

	/**
	 * extract the map id of a object
	 *
	 * @param id id
	 * @return new id
	 */
	public long extractObjects(long id) {
		return extract(objects, id);
	}

	private long extract(LongArray array, long id) {
		checkShared();
		// compute shared if required
		return CompressUtil.computeSharedNode(array.get(id), shared);
	}

	public long getTripleCount() {
		return tripleCount;
	}
}
