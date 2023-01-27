package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * create a negation bitmap of another one, the close method is called if the
 * bitmap implements closeable access/set are negate when called
 *
 * @param <T> bitmap type
 */
public class NegBitmap<T extends Bitmap> implements Bitmap, Closeable {

	/**
	 * create from a bitmap
	 *
	 * @param bitmap bitmap
	 * @return bitmap
	 */
	public static Bitmap of(Bitmap bitmap) {
		return new NegBitmap<>(bitmap);
	}

	/**
	 * create from a modifiableBitmap
	 *
	 * @param modifiableBitmap modifiableBitmap
	 * @return modifiableBitmap
	 */
	public static ModifiableBitmap of(ModifiableBitmap modifiableBitmap) {
		return new NegModifiableBitmap(modifiableBitmap);
	}

	private static class NegModifiableBitmap extends NegBitmap<ModifiableBitmap> implements ModifiableBitmap {

		private NegModifiableBitmap(ModifiableBitmap handle) {
			super(handle);
		}

		@Override
		public void set(long position, boolean value) {
			handle.set(position, !value);
		}

		@Override
		public void append(boolean value) {
			handle.append(!value);
		}
	}

	protected final T handle;

	private NegBitmap(T handle) {
		this.handle = handle;
	}

	@Override
	public boolean access(long position) {
		return !handle.access(position);
	}

	@Override
	public long rank1(long position) {
		return handle.rank0(position);
	}

	@Override
	public long rank0(long position) {
		return handle.rank1(position);
	}

	@Override
	public long selectPrev1(long start) {
		throw new NotImplementedException();
	}

	@Override
	public long selectNext1(long start) {
		throw new NotImplementedException();
	}

	@Override
	public long select0(long n) {
		return handle.select1(n);
	}

	@Override
	public long select1(long n) {
		return handle.select0(n);
	}

	@Override
	public long getNumBits() {
		return handle.getNumBits();
	}

	@Override
	public long countOnes() {
		return handle.countZeros();
	}

	@Override
	public long countZeros() {
		return handle.countOnes();
	}

	@Override
	public long getSizeBytes() {
		return handle.getSizeBytes();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public String getType() {
		return handle.getType();
	}

	@Override
	public void close() throws IOException {
		if (handle instanceof Closeable) {
			((Closeable) handle).close();
		}
	}
}
