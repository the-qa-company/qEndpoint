package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class MultiLayerBitmapWrapper implements MultiLayerBitmap, Closeable {
	public static class MultiLayerModBitmapWrapper extends MultiLayerBitmapWrapper
			implements ModifiableMultiLayerBitmap {

		private MultiLayerModBitmapWrapper(ModifiableBitmap handle, long graphs) {
			super(handle, graphs);
		}

		@Override
		public void set(long layer, long position, boolean value) {
			((ModifiableBitmap) handle).set(graphs * position + layer, value);
		}
	}

	public static MultiLayerBitmapWrapper of(Bitmap bitmap, long graphCount) {
		if (bitmap == null) {
			return null;
		}

		if (bitmap instanceof ModifiableBitmap mbm) {
			return new MultiLayerModBitmapWrapper(mbm, graphCount);
		}

		return new MultiLayerBitmapWrapper(bitmap, graphCount);
	}

	public static MultiLayerModBitmapWrapper of(ModifiableBitmap bitmap, long graphCount) {
		if (bitmap == null) {
			return null;
		}

		return new MultiLayerModBitmapWrapper(bitmap, graphCount);
	}

	protected final Bitmap handle;
	protected final long graphs;

	private MultiLayerBitmapWrapper(Bitmap handle, long graphs) {
		this.handle = handle;
		this.graphs = graphs;
	}

	@SuppressWarnings("unchecked")
	public <T extends Bitmap> T getHandle() {
		return (T) handle;
	}

	@Override
	public boolean access(long layer, long position) {
		return handle.access(graphs * position + layer);
	}

	@Override
	public long rank1(long layer, long position) {
		throw new NotImplementedException();
	}

	@Override
	public long rank0(long layer, long position) {
		throw new NotImplementedException();
	}

	@Override
	public long selectPrev1(long layer, long start) {
		throw new NotImplementedException();
	}

	@Override
	public long selectNext1(long layer, long start) {
		throw new NotImplementedException();
	}

	@Override
	public long select0(long layer, long n) {
		throw new NotImplementedException();
	}

	@Override
	public long select1(long layer, long n) {
		throw new NotImplementedException();
	}

	@Override
	public long getNumBits() {
		return (handle.getNumBits() - 1) / 5 + 1;
	}

	@Override
	public long countOnes(long layer) {
		throw new NotImplementedException();
	}

	@Override
	public long countZeros(long layer) {
		throw new NotImplementedException();
	}

	@Override
	public long getSizeBytes() {
		return handle.getSizeBytes();
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		handle.save(output, listener);
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		handle.load(input, listener);
	}

	@Override
	public String getType() {
		return handle.getType();
	}

	@Override
	public long getLayersCount() {
		return graphs;
	}

	@Override
	public boolean access(long position) {
		return access(0, position);
	}

	@Override
	public long rank1(long position) {
		throw new NotImplementedException();
	}

	@Override
	public long rank0(long position) {
		throw new NotImplementedException();
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
		throw new NotImplementedException();
	}

	@Override
	public long select1(long n) {
		return MultiLayerBitmap.super.select1(n);
	}

	@Override
	public long countOnes() {
		return MultiLayerBitmap.super.countOnes();
	}

	@Override
	public long countZeros() {
		return MultiLayerBitmap.super.countZeros();
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeObject(handle);
	}
}
