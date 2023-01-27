package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * sync version of a bitmap
 *
 * @param <T> bitmap
 */
public class SyncBitmap<T extends Bitmap> implements Bitmap, Closeable {
	/**
	 * create sync bitmap from a bitmap
	 *
	 * @param bitmap bitmap
	 * @return sync bitmap
	 */
	public static Bitmap of(Bitmap bitmap) {
		if (bitmap instanceof SyncBitmap) {
			return bitmap;
		}
		if (bitmap instanceof ModifiableBitmap) {
			return new SyncModBitmap<>((ModifiableBitmap) bitmap);
		}

		return new SyncBitmap<>(bitmap);
	}

	/**
	 * create sync mod bitmap from a mod bitmap
	 *
	 * @param bitmap bitmap
	 * @return sync mod bitmap
	 */
	public static ModifiableBitmap of(ModifiableBitmap bitmap) {
		if (bitmap instanceof SyncBitmap) {
			return bitmap;
		}
		return new SyncModBitmap<>(bitmap);
	}

	protected final T bitmap;

	private SyncBitmap(T bitmap) {
		this.bitmap = bitmap;
	}

	@Override
	public synchronized boolean access(long position) {
		return bitmap.access(position);
	}

	@Override
	public synchronized long rank1(long position) {
		return bitmap.rank1(position);
	}

	@Override
	public synchronized long rank0(long position) {
		return bitmap.rank0(position);
	}

	@Override
	public synchronized long selectPrev1(long start) {
		return bitmap.selectPrev1(start);
	}

	@Override
	public synchronized long selectNext1(long start) {
		return bitmap.selectNext1(start);
	}

	@Override
	public synchronized long select0(long n) {
		return bitmap.select0(n);
	}

	@Override
	public synchronized long select1(long n) {
		return bitmap.select1(n);
	}

	@Override
	public synchronized long getNumBits() {
		return bitmap.getNumBits();
	}

	@Override
	public synchronized long countOnes() {
		return bitmap.countOnes();
	}

	@Override
	public synchronized long countZeros() {
		return bitmap.countZeros();
	}

	@Override
	public synchronized long getSizeBytes() {
		return bitmap.getSizeBytes();
	}

	@Override
	public synchronized void save(OutputStream output, ProgressListener listener) throws IOException {
		bitmap.save(output, listener);
	}

	@Override
	public synchronized void load(InputStream input, ProgressListener listener) throws IOException {
		bitmap.load(input, listener);
	}

	@Override
	public synchronized String getType() {
		return bitmap.getType();
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeObject(bitmap);
	}

	private static class SyncModBitmap<T extends ModifiableBitmap> extends SyncBitmap<T> implements ModifiableBitmap {

		protected SyncModBitmap(T bitmap) {
			super(bitmap);
		}

		@Override
		public synchronized void set(long position, boolean value) {
			bitmap.set(position, value);
		}

		@Override
		public synchronized void append(boolean value) {
			bitmap.append(value);
		}
	}
}
