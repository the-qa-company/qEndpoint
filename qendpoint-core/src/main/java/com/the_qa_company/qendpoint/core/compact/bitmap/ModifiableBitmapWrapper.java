package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * wrapper for {@link ModifiableBitmap}
 *
 * @author Antoine Willerval
 */
public class ModifiableBitmapWrapper implements ModifiableBitmap, Closeable {
	protected final ModifiableBitmap wrapper;

	public ModifiableBitmapWrapper(ModifiableBitmap wrapper) {
		this.wrapper = wrapper;
	}

	@Override
	public boolean access(long position) {
		return wrapper.access(position);
	}

	@Override
	public long rank1(long position) {
		return wrapper.rank1(position);
	}

	@Override
	public long rank0(long position) {
		return wrapper.rank0(position);
	}

	@Override
	public long selectPrev1(long start) {
		return wrapper.selectPrev1(start);
	}

	@Override
	public long selectNext1(long start) {
		return wrapper.selectNext1(start);
	}

	@Override
	public long select0(long n) {
		return wrapper.select0(n);
	}

	@Override
	public long select1(long n) {
		return wrapper.select1(n);
	}

	@Override
	public long getNumBits() {
		return wrapper.getNumBits();
	}

	@Override
	public long countOnes() {
		return wrapper.countOnes();
	}

	@Override
	public long countZeros() {
		return wrapper.countZeros();
	}

	@Override
	public long getSizeBytes() {
		return wrapper.getSizeBytes();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		wrapper.save(output, listener);
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		wrapper.load(input, listener);
	}

	@Override
	public String getType() {
		return wrapper.getType();
	}

	@Override
	public void set(long position, boolean value) {
		wrapper.set(position, value);
	}

	@Override
	public void append(boolean value) {
		wrapper.append(value);
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeObject(wrapper);
	}
}
