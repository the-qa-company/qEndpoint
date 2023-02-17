package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class BitSequence375Test {
	private static final String MEMORY = "memory";
	private static final String DISK = "disk";
	private static final String FULL_DISK = "full_disk";

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> params() {
		return List.of(MEMORY, DISK, FULL_DISK);
	}

	static final long num = 10000;
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Parameterized.Parameter
	public String disk;
	Bitmap375Big bitseq;
	BitSet bitset;
	Closer add;

	@Before
	public void setUp() throws Exception {
		Random r = new Random(1);

		add = Closer.of();

		switch (disk) {
		case MEMORY: {
			bitseq = Bitmap375Big.memory(num);
			break;
		}
		case DISK: {
			Path file = tempDir.getRoot().toPath().resolve("test.bin");
			bitseq = Bitmap375Big.disk(file, num, false);
			add.with(CloseSuppressPath.of(file));
			break;
		}
		case FULL_DISK: {
			Path file = tempDir.getRoot().toPath().resolve("test.bin");
			bitseq = Bitmap375Big.disk(file, num, true);
			add.with(CloseSuppressPath.of(file));
			break;
		}
		default:
			throw new AssertionError();
		}
		bitset = new BitSet();

		for (int i = 0; i < num; i++) {
			boolean value = r.nextBoolean();
			bitset.set(i, value);
			bitseq.set(i, value);
		}
	}

	@After
	public void closer() throws IOException {
		Closer.closeAll(bitseq, bitset);
	}

	// Naive impl of select0
//		public long select0b(long x) {
//			if(x<=0) {
//				return -1;
//			}
//			long numzeros=0;
//			for(long i=0;i<getNumBits();i++) {
//				if(!access(i)) {
//					numzeros++;
//				}
//				if(numzeros==x) {
//					return i;
//				}
//			}
//			return getNumBits();
//		}

	@Test
	public void testLoadSave() {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			bitseq.save(out, null);

			ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

			try (Bitmap375Big loaded = Bitmap375Big.memory(0)) {
				loaded.load(in, null);

				assertEquals("Save/Load different number of elements", bitseq.getNumBits(), loaded.getNumBits());
				for (int i = 0; i < bitseq.getNumBits(); i++) {
					assertEquals("Save/Load different value", bitseq.access(i), loaded.access(i));
				}
			}
		} catch (IOException e) {
			fail("Exception thrown: " + e);
		}

	}

	@Test
	public void testNumbits() {
		assertEquals(0, Bitmap64Big.lastWordNumBits(0));
		assertEquals(1, Bitmap64Big.lastWordNumBits(1));
		assertEquals(64, Bitmap64Big.lastWordNumBits(64));
		assertEquals(1, Bitmap64Big.lastWordNumBits(65));
		assertEquals(64, Bitmap64Big.lastWordNumBits(128));
		assertEquals(1, Bitmap64Big.lastWordNumBits(129));

//		assertEquals(0, Bitmap64.numWords(0));
		assertEquals(1, Bitmap64Big.numWords(1));
		assertEquals(1, Bitmap64Big.numWords(64));
		assertEquals(2, Bitmap64Big.numWords(65));
		assertEquals(5, Bitmap64Big.numWords(257));

//		for(int i=0;i<1000;i++) {
//			int words=(int) Bitmap64.numWords(i);
//			int bitsLast=Bitmap64.lastWordNumBits(i);
//			int bytes = (int) Bitmap64.numBytes(i);
//			System.out.println(i+" bits  "+bytes+" bytes  "+words+" words / "+bitsLast+" bits last.");
//		}
	}

	@Test
	public void testSize() {
		assertEquals(bitseq.getNumBits(), num);
	}

	@Test
	public void testAccess() {
		for (int i = 0; i < bitset.size(); i++) {
			assertEquals(bitset.get(i), bitseq.access(i));
		}
	}

	@Test
	public void testRank1() {
		long count = 0;
		for (long i = 0; i < bitseq.getNumBits(); i++) {
			if (bitseq.access(i)) {
				count++;
//				assertEquals("Wrong rank1", count, bitseq.rank1(i));
//				assertEquals("Wrong rank new", bitseq.rank1(i), bitnew.rank1(i));

				if (bitseq.rank1(i) != count) {
					System.out.println("Different");

					long a = bitseq.rank1(i);
				}

				assertEquals(count, bitseq.rank1(i));
			}
		}
	}

	@Test
	public void testSelect1() {
		for (long i = 0; i < bitseq.getNumBits(); i++) {

//			System.out.print("***"+i+"> "+(bitseq.access(i)?1:0)+ " \t  Rank1("+i+"): "+ bitseq.rank1(i) + " Select1("+bitseq.rank1(i)+"): " + bitseq.select1(bitseq.rank1(i))+ " SelectNext1("+i+"): "+ bitseq.selectNext1(i));
			if (bitseq.access(i)) {
				assertEquals("Select1 wrong", i, (bitseq.select1(bitseq.rank1(i))));
			}
		}
	}

	@Test
	public void testSelectNext1() {
		for (long i = 0; i < bitseq.getNumBits(); i++) {
//			System.out.println("Pos: "+i+" => "+ (bitseq.access(i)?"1":"0")+ " Next: "+bitseq.selectNext1(i));
		}
	}

	public long select0b(ModifiableBitmap bitseq, long x) {
		if (x <= 0) {
			return -1;
		}
		long numzeros = 0;
		for (long i = 0; i < bitseq.getNumBits(); i++) {
			if (!bitseq.access(i)) {
				numzeros++;
			}
			if (numzeros == x) {
				return i;
			}
		}
		return bitseq.getNumBits();
	}

	@Test
	public void testSelect0() {
		long numzeros = 0;
		for (long i = 0; i < bitseq.getNumBits(); i++) {

//			System.out.print("***"+i+"> "+(bitseq.access(i)?1:0)+ " \t  Rank1("+i+"): "+ bitseq.rank1(i) + " Select1("+bitseq.rank1(i)+"): " + bitseq.select1(bitseq.rank1(i))+ " SelectNext1("+i+"): "+ bitseq.selectNext1(i));
			if (bitseq.access(i)) {
//				assertEquals("Select0 wrong", i, (bitseq.select0(bitseq.rank0(i))));
			} else {
				numzeros++;
			}
			long rk = bitseq.rank0(i);
			long sel = bitseq.select0(rk);
			long selb = select0b(bitseq, rk);
//			System.out.println(i+" => "+ (bitseq.access(i) ? "1" : "0") +" Zeros: "+numzeros+" Rank0="+rk+ " Sel0="+sel+" Sel0b="+selb);
			assertEquals("Select0 wrong", sel, selb);
		}
	}

	@Test
	public void testCountOnes() {
		int count = 0;
		for (int i = 0; i < bitseq.getNumBits(); i++) {
			if (bitseq.access(i)) {
				count++;
			}
		}
		assertEquals("Wrong count ones", count, bitseq.countOnes());
	}

	@Test
	public void testCountZeros() {
		int count = 0;
		for (int i = 0; i < bitseq.getNumBits(); i++) {
			if (!bitseq.access(i)) {
				count++;
			}
		}
		assertEquals("Wrong count zeros", count, bitseq.countZeros());
	}

	@Test
	public void testBitutilSelect0() {
//		IOUtil.printBitsln(bitseq.words[0],64);

		long word = bitseq.words.get(0);

		int numbits = 64 - Long.bitCount(word);
//		for(int i=1;i<=numbits;i++) {
//			System.out.println("Select0("+i+") = "+BitUtil.select0(bitseq.words[0], i));
//		}
	}
}
