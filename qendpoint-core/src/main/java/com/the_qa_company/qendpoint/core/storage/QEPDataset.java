package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.compact.bitmap.AddSnapshotBitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.storage.iterator.CloseableIterator;
import com.the_qa_company.qendpoint.core.storage.iterator.QueryCloseableIterator;
import com.the_qa_company.qendpoint.core.storage.search.QEPComponentTriple;
import com.the_qa_company.qendpoint.core.storage.search.QEPDatasetIterator;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.util.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dataset information
 *
 * @author Antoine Willerval
 * @see QEPCore
 */
public class QEPDataset implements Closeable {

	public record ComponentFind(QEPDataset dataset, TripleComponentRole role, long id, long pid) {
		public boolean isFind() {
			return role != null || pid != 0;
		}

		@Override
		public String toString() {
			StringBuilder buffer = new StringBuilder("ComponentFind:").append(dataset.uid);
			if (!isFind()) {
				buffer.append("[unknown]");
			} else {
				if (pid != 0) {
					buffer.append("[pid=").append(pid).append("]");
				}
				if (role != null) {
					buffer.append("[id=").append(id).append("/").append(role).append("]");
				}
			}

			return buffer.toString();
		}
	}

	private static final AtomicInteger DATASET_UID_FETCHER = new AtomicInteger();
	private final QEPCore core;
	private final String id;
	private final Path path;
	private final HDT dataset;
	private final AddSnapshotBitmap deleteBitmap;
	private final ModifiableBitmap[] deltaBitmaps;
	private final int uid;

	/**
	 * @param core         core
	 * @param id           string id of the dataset
	 * @param path         path of the dataset
	 * @param dataset      loaded dataset
	 * @param deleteBitmap delete bitmap
	 * @param deltaBitmaps delta bitmaps for the dataset
	 */
	public QEPDataset(QEPCore core, String id, Path path, HDT dataset, ModifiableBitmap deleteBitmap,
			ModifiableBitmap[] deltaBitmaps) {
		this.core = core;
		this.id = id;
		this.path = path;
		this.dataset = dataset;
		this.deleteBitmap = AddSnapshotBitmap.of(deleteBitmap);
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

	public ComponentFind find(CharSequence seq) {
		long pid = Math.max(0, dataset.getDictionary().stringToId(seq, TripleComponentRole.PREDICATE));

		long sid = dataset.getDictionary().stringToId(seq, TripleComponentRole.SUBJECT);
		if (sid > 0) {
			return new ComponentFind(this, TripleComponentRole.SUBJECT, sid, pid);
		}
		long oid = dataset.getDictionary().stringToId(seq, TripleComponentRole.OBJECT);
		if (oid > 0) {
			return new ComponentFind(this, TripleComponentRole.OBJECT, oid, pid);
		}
		return new ComponentFind(this, null, 0, pid);
	}

	/**
	 * @return the snapshot delete bitmap
	 */
	public AddSnapshotBitmap deleteBitmap() {
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
	 * @return a new context where no elements from this dataset will be deleted
	 */
	public QEPDatasetContext createContext() {
		final AddSnapshotBitmap.AddSnapshotDeltaBitmap bm = deleteBitmap.createSnapshot();
		return new QEPDatasetContext() {
			@Override
			public boolean isTripleDeleted(long tripleID) {
				if (tripleID < 0 || tripleID > dataset.getTriples().getNumberOfElements()) {
					throw new IllegalArgumentException("bad triple id: " + tripleID);
				}
				return bm.access(tripleID);
			}

			@Override
			public Bitmap deleteBitmap() {
				return bm;
			}

			@Override
			public QEPDataset dataset() {
				return QEPDataset.this;
			}

			@Override
			public void close() {
				bm.close();
			}
		};
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
	public QueryCloseableIterator search(CharSequence subject, CharSequence predicate,
			CharSequence object) throws QEPCoreException {
		QEPDatasetContext ctx = createContext();
		return search(ctx, subject, predicate, object).attach(ctx);
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
	public QueryCloseableIterator search(QEPComponent subject, QEPComponent predicate,
			QEPComponent object) throws QEPCoreException {
		QEPDatasetContext ctx = createContext();
		return search(ctx, subject, predicate, object).attach(ctx);
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
	public QueryCloseableIterator search(QEPComponentTriple pattern)
			throws QEPCoreException {
		QEPDatasetContext ctx = createContext();
		return search(ctx, pattern).attach(ctx);
	}

	/**
	 * search a triple pattern over the dataset with strings
	 *
	 * @param context   dataset context
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator
	 * @throws QEPCoreException search exception
	 * @see #search(QEPComponentTriple)
	 * @see #search(QEPComponent, QEPComponent, QEPComponent)
	 */
	public QueryCloseableIterator search(QEPDatasetContext context,
			CharSequence subject, CharSequence predicate, CharSequence object) throws QEPCoreException {
		return search(context, core.createComponentByString(subject), core.createComponentByString(predicate),
				core.createComponentByString(object));
	}

	/**
	 * search a triple pattern over the dataset with component
	 *
	 * @param context   dataset context
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 * @return iterator
	 * @throws QEPCoreException search exception
	 * @see #search(QEPComponentTriple)
	 * @see #search(CharSequence, CharSequence, CharSequence)
	 */
	public QueryCloseableIterator search(QEPDatasetContext context,
			QEPComponent subject, QEPComponent predicate, QEPComponent object) throws QEPCoreException {
		return search(context, QEPComponentTriple.of(subject, predicate, object));
	}

	/**
	 * search a triple pattern over the dataset
	 *
	 * @param context dataset context
	 * @param pattern pattern
	 * @return iterator
	 * @throws QEPCoreException search exception
	 * @see #search(CharSequence, CharSequence, CharSequence)
	 * @see #search(QEPComponent, QEPComponent, QEPComponent)
	 */
	public QueryCloseableIterator search(QEPDatasetContext context,
	                                     QEPComponentTriple pattern) throws QEPCoreException {
		// freeze the components to avoid recomputing already known values
		QEPComponentTriple clone = pattern.freeze();
		// search over the dataset
		clone.setDatasetId(uid);
		IteratorTripleID it = dataset.getTriples().search(clone.tripleID(this));
		return new QEPDatasetIterator(context, it, clone);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (QEPDataset) obj;
		return Objects.equals(this.id, that.id) && Objects.equals(this.path, that.path) && this.uid == that.uid;
	}

	@Override
	public int hashCode() {
		return Objects.hash(path);
	}

	@Override
	public String toString() {
		return "QEPDataset[" + "id=" + id + ", " + "path=" + path + ", " + "uid=" + uid + ']';
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
