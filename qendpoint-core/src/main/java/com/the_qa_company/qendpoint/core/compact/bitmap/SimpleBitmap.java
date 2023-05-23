package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * interface throwing {@link NotImplementedException} for all the complex bitmap
 * features
 *
 * @author Antoine Willerval
 */
public interface SimpleBitmap extends Bitmap {
	@Override
	default long rank1(long position) {
		throw new NotImplementedException();
	}

	@Override
	default long rank0(long pos) {
		return pos + 1L - rank1(pos);
	}

	@Override
	default long selectPrev1(long start) {
		throw new NotImplementedException();
	}

	@Override
	default long selectNext1(long start) {
		throw new NotImplementedException();
	}

	@Override
	default long select0(long n) {
		throw new NotImplementedException();
	}

	@Override
	default long select1(long n) {
		throw new NotImplementedException();
	}

	@Override
	default long countOnes() {
		throw new NotImplementedException();
	}

	@Override
	default long countZeros() {
		throw new NotImplementedException();
	}

	@Override
	default void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	default void save(OutputStream output, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}
}
