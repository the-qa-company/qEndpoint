package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.dictionary.impl.CompressFourSectionDictionary;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.disk.LongArray;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressUtil;
import com.the_qa_company.qendpoint.core.util.io.compress.WriteLongArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;

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
	private final WriteLongArrayBuffer graph;
	private final CloseSuppressPath locationSubjects;
	private final CloseSuppressPath locationPredicates;
	private final CloseSuppressPath locationObjects;
	private final CloseSuppressPath locationGraph;
	private long shared = -1;
	private final long tripleCount;
	private final boolean quads;
	private final long graphs;

	public CompressTripleMapper(CloseSuppressPath location, long tripleCount, long chunkSize, boolean quads,
			long graphs) {
		this.tripleCount = tripleCount;
		this.quads = quads;

		locationSubjects = location.resolve("map_subjects");
		locationPredicates = location.resolve("map_predicates");
		locationObjects = location.resolve("map_objects");
		locationGraph = location.resolve("map_graph");
		this.graphs = graphs;
		int numbits = BitUtil.log2(tripleCount + 2) + CompressUtil.INDEX_SHIFT;
		int maxElement = (int) Math.min(chunkSize / Long.BYTES / 3, Integer.MAX_VALUE - 5);
		try {
			Files.deleteIfExists(locationSubjects);
			Files.deleteIfExists(locationPredicates);
			Files.deleteIfExists(locationObjects);
			if (quads) {
				Files.deleteIfExists(locationGraph);
			}
		} catch (IOException e) {
			throw new RuntimeException("Can't reset disk mapping files", e);
		}
		subjects = new WriteLongArrayBuffer(
				new SequenceLog64BigDisk(locationSubjects, numbits, tripleCount + 2, true, false), tripleCount,
				maxElement);
		predicates = new WriteLongArrayBuffer(
				new SequenceLog64BigDisk(locationPredicates, numbits, tripleCount + 2, true, false), tripleCount,
				maxElement);
		objects = new WriteLongArrayBuffer(
				new SequenceLog64BigDisk(locationObjects, numbits, tripleCount + 2, true, false), tripleCount,
				maxElement);
		if (quads) {
			graph = new WriteLongArrayBuffer(
					new SequenceLog64BigDisk(locationGraph, numbits, tripleCount + 2, true, false), tripleCount,
					maxElement);
		} else {
			graph = null;
		}
	}

	/**
	 * delete the map files and the location files
	 */
	public void delete() {
		try {
			IOUtil.closeAll(subjects, predicates, objects, graph);
		} catch (IOException e) {
			log.warn("Can't close triple map array", e);
		}
		try {
			IOUtil.closeAll(locationSubjects, locationPredicates, locationObjects, locationGraph);
		} catch (IOException e) {
			log.warn("Can't delete triple map array files", e);
		}
	}

	@Override
	public void onSubject(long preMapId, long newMapId) {
		subjects.set(preMapId, newMapId);
	}

	@Override
	public void onSubject(long[] preMapIds, long[] newMapIds, int offset, int length) {
		subjects.set(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onPredicate(long preMapId, long newMapId) {
		predicates.set(preMapId, newMapId);
	}

	@Override
	public void onPredicate(long[] preMapIds, long[] newMapIds, int offset, int length) {
		predicates.set(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onObject(long preMapId, long newMapId) {
		objects.set(preMapId, newMapId);
	}

	@Override
	public void onObject(long[] preMapIds, long[] newMapIds, int offset, int length) {
		objects.set(preMapIds, newMapIds, offset, length);
	}

	@Override
	public void onGraph(long preMapId, long newMapId) {
		graph.set(preMapId, newMapId);
	}

	@Override
	public void onGraph(long[] preMapIds, long[] newMapIds, int offset, int length) {
		graph.set(preMapIds, newMapIds, offset, length);
	}

	public void setShared(long shared) {
		this.shared = shared;
		subjects.free();
		predicates.free();
		objects.free();
		if (supportsGraph()) {
			graph.free();
		}
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

	/**
	 * extract the map id of a graph
	 *
	 * @param id id
	 * @return new id
	 */
	public long extractGraph(long id) {
		return extract(graph, id) - shared;
	}

	private long extract(LongArray array, long id) {
		checkShared();
		// compute shared if required
		return CompressUtil.computeSharedNode(array.get(id), shared);
	}

	public long getTripleCount() {
		return tripleCount;
	}

	public boolean supportsGraph() {
		return quads;
	}

	public long getGraphsCount() {
		return graphs;
	}
}
