package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;
import com.the_qa_company.qendpoint.core.storage.search.QEPDatasetIterator;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dataset information
 *
 * @author Antoine Willerval
 * @see QEPCore
 */
public class QEPDataset implements Closeable {
	private static final AtomicInteger DATASET_UID_FETCHER = new AtomicInteger();
	private final QEPCore core;
	private final String id;
	private final Path path;
	private final HDT dataset;
	private final ModifiableBitmap deleteBitmap;
	private final ModifiableBitmap[] deltaBitmaps;
	private final int uid;

	/**
	 * @param core         core
	 * @param id           string id of the dataset
	 * @param path         path of the dataset
	 * @param dataset      loaded dataset
	 * @param deleteBitmap delete bitmap
	 * @param deltaBitmaps
	 */
	public QEPDataset(QEPCore core, String id, Path path, HDT dataset, ModifiableBitmap deleteBitmap, ModifiableBitmap[] deltaBitmaps) {
		this.core = core;
		this.id = id;
		this.path = path;
		this.dataset = dataset;
		this.deleteBitmap = deleteBitmap;
		this.deltaBitmaps = deltaBitmaps;
		this.uid = DATASET_UID_FETCHER.incrementAndGet();
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(dataset, deleteBitmap, deltaBitmaps);
	}

	public QEPCore core() {
		return core;
	}

	public String id() {
		return id;
	}

	public Path path() {
		return path;
	}

	public HDT dataset() {
		return dataset;
	}

	public int uid() {
		return uid;
	}

	public ModifiableBitmap deleteBitmap() {
		return deleteBitmap;
	}

	/**
	 * delete a triple from its id
	 *
	 * @param tripleID the triple ID
	 * @see #isTripleDeleted(long)
	 */
	public void deleteTriple(long tripleID) {
		if (tripleID < 0 || tripleID > dataset.getTriples().getNumberOfElements()) {
			throw new IllegalArgumentException("bad triple id: " + tripleID);
		}
		deleteBitmap.set(tripleID, true);
	}

	/**
	 * test if a triple is deleted
	 *
	 * @param tripleID triple id
	 * @return if the triple is deleted
	 * @see #deleteTriple(long)
	 */
	public boolean isTripleDeleted(long tripleID) {
		if (tripleID < 0 || tripleID > dataset.getTriples().getNumberOfElements()) {
			throw new IllegalArgumentException("bad triple id: " + tripleID);
		}
		return deleteBitmap.access(tripleID);
	}

	/**
	 * is a component is in another store
	 *
	 * @param role      role
	 * @param component component id
	 * @return true if this component is in another store, false otherwise
	 * @see #setComponentInDelta(TripleComponentRole, long)
	 */
	public boolean isComponentInDelta(TripleComponentRole role, long component) {
		if (component < 0) {
			return false;
		}
		if (role == TripleComponentRole.OBJECT) {
			long nshared = dataset.getDictionary().getNshared();
			if (component <= nshared) {
				return isComponentInDelta(TripleComponentRole.SUBJECT, component);
			}
			ModifiableBitmap bm = deltaBitmaps[TripleComponentRole.OBJECT.ordinal()];
			return bm.access(component - nshared + 1);
		}
		// predicates/subjects
		if (component > dataset.getDictionary().getNSection(role)) {
			return false;
		}

		ModifiableBitmap bm = deltaBitmaps[role.ordinal()];
		return bm.access(component);
	}

	/**
	 * set a component in the delta
	 *
	 * @param role      the role of this component
	 * @param component the component
	 * @see #isComponentInDelta(TripleComponentRole, long)
	 */
	public void setComponentInDelta(TripleComponentRole role, long component) {
		if (component < 0) {
			return;
		}

		if (role == TripleComponentRole.OBJECT) {
			long nshared = dataset.getDictionary().getNshared();
			if (component <= nshared) {
				setComponentInDelta(TripleComponentRole.SUBJECT, component);
				return;
			}
			if (component > dataset.getDictionary().getNSection(role)) {
				return;
			}
			ModifiableBitmap bm = deltaBitmaps[TripleComponentRole.OBJECT.ordinal()];
			bm.set(component - nshared + 1, true);
			return;
		}
		// predicates/subjects
		if (component > dataset.getDictionary().getNSection(role)) {
			return;
		}

		ModifiableBitmap bm = deltaBitmaps[role.ordinal()];
		bm.set(component, true);
	}

	/**
	 * search a triple pattern over the dataset
	 *
	 * @param pattern pattern
	 * @return iterator
	 * @throws QEPCoreException search exception
	 * @see #search(CharSequence, CharSequence, CharSequence)
	 * @see #search(QEPComponent, QEPComponent, QEPComponent)
	 */
	public Iterator<QEPComponentTriple> search(QEPComponentTriple pattern) throws QEPCoreException {
		// freeze the components to avoid recomputing already known values
		QEPComponentTriple clone = pattern.freeze();
		// search over the dataset
		clone.setDatasetId(uid);
		IteratorTripleID it = dataset.getTriples().search(clone.tripleID(this));
		return new QEPDatasetIterator(this, it, clone);
	}

	/**
	 * search a triple pattern over the dataset with component
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator
	 * @throws QEPCoreException search exception
	 * @see #search(QEPComponentTriple)
	 * @see #search(CharSequence, CharSequence, CharSequence)
	 */
	public Iterator<QEPComponentTriple> search(QEPComponent subject, QEPComponent predicate, QEPComponent object) throws QEPCoreException {
		return search(QEPComponentTriple.of(subject, predicate, object));
	}

	/**
	 * search a triple pattern over the dataset with strings
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator
	 * @throws QEPCoreException search exception
	 * @see #search(QEPComponentTriple)
	 * @see #search(QEPComponent, QEPComponent, QEPComponent)
	 */
	public Iterator<QEPComponentTriple> search(CharSequence subject, CharSequence predicate, CharSequence object) throws QEPCoreException {
		return search(
				core.createComponentByString(subject),
				core.createComponentByString(predicate),
				core.createComponentByString(object)
		);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (obj == null || obj.getClass() != this.getClass()) return false;
		var that = (QEPDataset) obj;
		return Objects.equals(this.id, that.id) &&
				Objects.equals(this.path, that.path) &&
				this.uid == that.uid;
	}

	@Override
	public int hashCode() {
		return Objects.hash(path);
	}

	@Override
	public String toString() {
		return "QEPDataset[" +
				"id=" + id + ", " +
				"path=" + path + ", " +
				"uid=" + uid + ']';
	}

	/**
	 * create a component from its id and its section role
	 *
	 * @param id   id
	 * @param role role
	 * @return component
	 * @see #component(long, TripleComponentRole)
	 */
	public QEPComponent component(long id, DictionarySectionRole role) {
		return new QEPComponent(core, this, role, id, null);
	}

	/**
	 * create a component from its id and its triple role
	 *
	 * @param id   id
	 * @param role role
	 * @return component
	 * @see #component(long, DictionarySectionRole)
	 */
	public QEPComponent component(long id, TripleComponentRole role) {
		DictionarySectionRole dr;
		if (role == TripleComponentRole.PREDICATE) {
			dr = DictionarySectionRole.PREDICATE;
		} else {
			if (id <= dataset.getDictionary().getNshared()) {
				dr = DictionarySectionRole.SHARED;
			} else {
				dr = role.asDictionarySectionRole();
			}
		}
		return new QEPComponent(core, this, dr, id, null);
	}

}