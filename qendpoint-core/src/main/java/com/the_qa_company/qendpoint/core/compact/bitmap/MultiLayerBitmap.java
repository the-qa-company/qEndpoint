package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface MultiLayerBitmap extends Bitmap {
    static MultiLayerBitmap ofBitmap(Bitmap bitmap) {
        if (bitmap instanceof MultiLayerBitmap mlb) {
            return mlb;
        }
        return new MultiLayerBitmap() {

            @Override
            public boolean access(long layer, long position) {
                return bitmap.access(position);
            }

            @Override
            public long rank1(long layer, long position) {
                return bitmap.rank1(position);
            }

            @Override
            public long rank0(long layer, long position) {
                return bitmap.rank0(position);
            }

            @Override
            public long selectPrev1(long layer, long start) {
                return bitmap.selectPrev1(start);
            }

            @Override
            public long selectNext1(long layer, long start) {
                return bitmap.selectNext1(start);
            }

            @Override
            public long select0(long layer, long n) {
                return bitmap.select0(n);
            }

            @Override
            public long select1(long layer, long n) {
                return bitmap.select1(n);
            }

            @Override
            public long getNumBits() {
                return bitmap.getNumBits();
            }

            @Override
            public long countOnes(long layer) {
                return bitmap.countOnes();
            }

            @Override
            public long countZeros(long layer) {
                return bitmap.countZeros();
            }

            @Override
            public long getSizeBytes() {
                return bitmap.getSizeBytes();
            }

            @Override
            public void save(OutputStream output, ProgressListener listener) throws IOException {
                bitmap.save(output, listener);
            }

            @Override
            public void load(InputStream input, ProgressListener listener) throws IOException {
                bitmap.load(input, listener);
            }

            @Override
            public String getType() {
                return bitmap.getType();
            }

            @Override
            public long getLayersCount() {
                return 1;
            }
        };
    }

    /**
     * Get the value of the bit at position pos
     *
     * @param layer layer
     * @param position pos
     * @return boolean
     */
    boolean access(long layer, long position);

    /**
     * Count the number of ones up to position pos (included)
     *
     * @param layer layer
     * @param position pos
     * @return long
     */
    long rank1(long layer, long position);

    /**
     * Count the number of zeros up to position pos (included)
     *
     * @param layer layer
     * @param position pos
     * @return long
     */
    long rank0(long layer, long position);

    /**
     * Return the position of the next 1 after position start.
     *
     * @param layer layer
     * @param start start
     * @return long
     */
    long selectPrev1(long layer, long start);

    /**
     * Return the position of the previous 1 before position start.
     *
     * @param layer layer
     * @param start start
     * @return long
     */
    long selectNext1(long layer, long start);

    /**
     * Find the position where n zeros have appeared up to that position.
     *
     * @param layer layer
     * @param n n
     * @return long
     */
    long select0(long layer, long n);

    /**
     * Find the position where n ones have appeared up to that position.
     *
     * @param layer layer
     * @param n n
     * @return long
     */
    long select1(long layer, long n);

    /**
     * Get number of total bits in the data structure
     *
     * @return long
     */
    long getNumBits();

    /**
     * Count the number of total ones in the data structure.
     *
     * @param layer layer
     * @return long
     */
    long countOnes(long layer);

    /**
     * Count the number of total zeros in the data structure.
     *
     * @param layer layer
     * @return long
     */
    long countZeros(long layer);

    /**
     * Estimate the size in bytes of the total data structure.
     *
     * @return long
     */
    long getSizeBytes();

    /**
     * Dump Bitmap into an {@link OutputStream}
     *
     * @param output   The OutputStream
     * @param listener Listener to get notified of loading progress. Can be null
     *                 if no notifications needed.
     * @throws IOException io exception while saving the bitmap
     */
    void save(OutputStream output, ProgressListener listener) throws IOException;

    /**
     * Load Bitmap from an {@link OutputStream}
     *
     * @param input    The OutputStream
     * @param listener Listener to get notified of loading progress. Can be null
     *                 if no notifications needed.
     * @throws IOException io exception while loading the bitmap
     */
    void load(InputStream input, ProgressListener listener) throws IOException;

    /**
     * @return the type of the data structure as defined in HDTVocabulary
     */
    String getType();

    /**
     * @return layers count
     */
    long getLayersCount();

    @Override
    default boolean access(long position) {
        return access(0, position);
    }

    @Override
    default long rank1(long position) {
        return rank1(0, position);
    }

    @Override
    default long rank0(long position) {
        return rank0(0, position);
    }

    @Override
    default long selectPrev1(long start) {
        return selectPrev1(0, start);
    }

    @Override
    default long selectNext1(long start) {
        return selectNext1(0, start);
    }

    @Override
    default long select0(long n) {
        return select0(0, n);
    }

    @Override
    default long select1(long n) {
        return select1(0, n);
    }

    @Override
    default long countOnes() {
        return countOnes(0);
    }

    @Override
    default long countZeros() {
        return countZeros(0);
    }
}
