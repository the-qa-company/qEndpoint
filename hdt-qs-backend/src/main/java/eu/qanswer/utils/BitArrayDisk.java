package eu.qanswer.utils;

import org.eclipse.rdf4j.common.io.NioFile;
import org.rdfhdt.hdt.compact.bitmap.Bitmap375;
import org.rdfhdt.hdt.util.io.IOUtil;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;

public class BitArrayDisk {

    protected final static int LOGW = 6;
    protected final static int W = 64;

    protected long numbits;
    protected long [] words;

    private static final int BLOCKS_PER_SUPER = 4;

    // Variables
    private long pop;
    private long [] superBlocksLong;
    private int [] superBlocksInt;
    private byte [] blocks;
    private boolean indexUpToDate;

    private String location;
    NioFile output;

    // only for testing we don't necessarily need to store the array on disk
    private boolean inMemory = false;


    public BitArrayDisk(long nbits,String location){
        this.numbits = 0;
        this.location = location;
        try {
            this.output = new NioFile(new File(location));
            initWordsArray(nbits);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public BitArrayDisk(long nbits,boolean inMemory){
        this.numbits = 0;
        this.inMemory = inMemory;
        initWordsArray(nbits);
    }
    public BitArrayDisk(long nbits,File file){
        this.numbits = 0;
        this.location = file.getAbsolutePath();
        try {
            this.output = new NioFile(file);
            initWordsArray(nbits);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWordsArray(long nbits){
        try {
            if(!inMemory) {
                if (output.size() == 0) { // file empty
                    int nwords = (int) numWords(nbits);
                    this.words = new long[nwords];
                    // write the length of the array in the beginning
                    this.output.writeLong(nwords, 0);
                } else {
                    // read the length of the array from the beginning
                    long length = this.output.readLong(0);
                    this.words = new long[(int) length];
                    for (int i = 0; i < length; i++) {
                        this.words[i] = this.output.readLong((i + 1) * 8);
                    }
                }
            }else{
                int nwords = (int) numWords(nbits);
                this.words = new long[nwords];
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static long numWords(long numbits) {
        return ((numbits-1)>>>LOGW) + 1;
    }
    protected static int wordIndex(long bitIndex) {
        return (int) (bitIndex >>> LOGW);
    }

    public boolean access(long bitIndex) {
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

        int wordIndex = wordIndex(bitIndex);
        if(wordIndex>=words.length) {
            return false;
        }

        return (words[wordIndex] & (1L << bitIndex)) != 0;
    }

    protected final void ensureSize(int wordsRequired) {
        if(words.length<wordsRequired) {
            long [] newWords = new long[Math.max(words.length*2, wordsRequired)];
            System.arraycopy(words, 0, newWords, 0, Math.min(words.length, newWords.length));
            words = newWords;
        }
    }

    public void set(long bitIndex, boolean value) {
        indexUpToDate = false;
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

        int wordIndex = wordIndex(bitIndex);
        ensureSize(wordIndex+1);

        if(value) {
            words[wordIndex] |= (1L << bitIndex);
        } else {
            words[wordIndex] &= ~(1L << bitIndex);
        }

        this.numbits = Math.max(this.numbits, bitIndex+1);
        if(!inMemory)
            writeToDisk(words[wordIndex],wordIndex);
    }
    private void writeToDisk(long l,int wordIndex){
        try {
            output.writeLong(l,(wordIndex + 1)*8); // +1 reserved for the length of the array
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void trimToSize() {
        int wordNum = (int) numWords(numbits);
        if(wordNum!=words.length) {
            words = Arrays.copyOf(words, wordNum);
        }
    }
    public void updateIndex() {
        trimToSize();
        if(numbits>Integer.MAX_VALUE) {
            superBlocksLong = new long[1+(words.length-1)/BLOCKS_PER_SUPER];
        } else {
            superBlocksInt = new int[1+(words.length-1)/BLOCKS_PER_SUPER];
        }
        blocks = new byte[words.length];

        long countBlock=0, countSuperBlock=0;
        int blockIndex=0, superBlockIndex=0;

        while(blockIndex<words.length) {
            if((blockIndex%BLOCKS_PER_SUPER)==0) {
                countSuperBlock += countBlock;
                if(superBlocksLong!=null) {
                    if(superBlockIndex<superBlocksLong.length) {
                        superBlocksLong[superBlockIndex++] = countSuperBlock;
                    }
                } else {
                    if(superBlockIndex<superBlocksInt.length) {
                        superBlocksInt[superBlockIndex++] = (int)countSuperBlock;
                    }
                }
                countBlock = 0;
            }
            blocks[blockIndex] = (byte) countBlock;
            countBlock += Long.bitCount(words[blockIndex]);
            blockIndex++;
        }
        pop = countSuperBlock+countBlock;
        indexUpToDate = true;
    }

    public long rank1(long pos) {
        if(pos<0) {
            return 0;
        }
        if(!indexUpToDate) {
            updateIndex();
        }
        if(pos>=numbits) {
            return pop;
        }

        long superBlockIndex = pos/(BLOCKS_PER_SUPER*W);
        long superBlockRank;
        if(superBlocksLong!=null) {
            superBlockRank = superBlocksLong[(int)superBlockIndex];
        } else {
            superBlockRank = superBlocksInt[(int)superBlockIndex];
        }

        long blockIndex = pos/W;
        long blockRank = 0xFF & blocks[(int)blockIndex];

        long chunkIndex = W-1-pos%W;
        long block = words[(int)blockIndex] << chunkIndex;
        long chunkRank = Long.bitCount(block);

        return superBlockRank + blockRank + chunkRank;
    }
    public long countOnes() {
        return rank1(numbits);
    }

    public long getNumbits() {
        return numbits;
    }
    public int getNumWords(){
        return this.words.length;
    }


    public String toString() {
        StringBuilder str = new StringBuilder();
        for(long i=0;i<numbits;i++) {
            str.append(access(i) ? '1' : '0');
        }
        return str.toString();
    }
    public void close(){
        try {
            this.output.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public void force(Boolean bool){
        try {
            this.output.force(bool);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}