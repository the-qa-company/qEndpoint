package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;

/**
 * a bitmap with snapshot capability, only the access and the add operations are
 * defined, a bit can't be removed
 *
 * @author Antoine Willerval
 */
public class AddSnapshotBitmap extends ModifiableBitmapWrapper {
	/**
	 * create a snapshot bitmap from a bitmap, all future set operation has to
	 * be done with the new created object, any add or remove made outside won't
	 * be considered by the snapshots.
	 *
	 * @param bitmap bitmap
	 * @return snapshot bitmap
	 */
	public static AddSnapshotBitmap of(ModifiableBitmap bitmap) {
		return new AddSnapshotBitmap(bitmap);
	}

	/**
	 * latest snapshot to be created
	 */
	private DeltaBitmap latest;
	/**
	 * mutex object to be thread safe, TODO: create more specific locks
	 */
	private final Object mutex = new Object() {};

	private AddSnapshotBitmap(ModifiableBitmap main) {
		super(main);
	}

	@Override
	public void set(long position, boolean value) {
		if (!value) {
			throw new NotImplementedException("Can't set 0 value in a AddSnapshotBitmap!");
		}
		// we mark this bit as set
		synchronized (mutex) {
			latest.snapshot.set(position, true);
		}
		super.set(position, true);
	}

	/**
	 * @return a snapshot bitmap to use to not see the next elements set to the
	 *         bitmap
	 */
	public AddSnapshotDeltaBitmap createSnapshot() {
		return new DeltaBitmap();
	}

	public interface AddSnapshotDeltaBitmap extends Bitmap, AutoCloseable {
		@Override
		void close();
	}

	private class DeltaBitmap implements SimpleBitmap, AddSnapshotDeltaBitmap {
		/**
		 * compressed memory bitmap storing the delta
		 */
		final RoaringBitmap snapshot = new RoaringBitmap();
		/**
		 * next snapshot created after this one
		 */
		DeltaBitmap next;
		/**
		 * previous snapshot created before this one
		 */
		DeltaBitmap prev;

		DeltaBitmap() {
			synchronized (mutex) {
				// set them
				prev = latest;
				if (prev != null) {
					prev.next = this;
				}
				latest = this;
			}
		}

		@Override
		public boolean access(long position) {
			if (// we check if the main bm contains the bit
			!wrapper.access(position)) {
				return false;
			}

			synchronized (mutex) {
				return access0(position);
			}
		}

		public boolean access0(long position) {
			return !snapshot.access(position)
					// if not, we check if a future snapshot contains this bit
					&& (next == null || next.access(position));
		}

		@Override
		public void close() {
			synchronized (mutex) {
				if (prev != null) {
					// merge the bitmap with the previous one
					prev.snapshot.getHandle().or(snapshot.getHandle());
					// we set the new next to our next snapshot (if any)
					prev.next = next;
				}

				if (next != null) {
					next.prev = prev;
				} else {
					assert latest == this;
					latest = prev;
				}
			}
		}

		@Override
		public long getNumBits() {
			return wrapper.getNumBits();
		}

		@Override
		public long getSizeBytes() {
			return AddSnapshotBitmap.this.getSizeBytes();
		}

		@Override
		public String getType() {
			return wrapper.getType();
		}
	}
}
