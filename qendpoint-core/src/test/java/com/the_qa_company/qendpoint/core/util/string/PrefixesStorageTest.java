package com.the_qa_company.qendpoint.core.util.string;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PrefixesStorageTest {
	public static void assertEqualsBS(CharSequence s1, CharSequence s2) {
		if (ByteString.of(s1).compareTo(ByteString.of(s2)) != 0) {
			fail(s1 + " != " + s2);
		}

	}

	@Test
	public void storageTest() {
		String cfg = String.join(";", "bbbb", "dddd", "ffff");

		PrefixesStorage st = new PrefixesStorage();
		st.loadConfig(cfg);

		assertEquals(3, st.getPrefixes().size());
		assertEquals(1, st.sizeof());

		assertEquals(0, st.prefixOf(ByteString.of("aaaa01")));
		assertEquals(0, st.prefixOf(ByteString.of("bbb01")));
		assertEquals(1, st.prefixOf(ByteString.of("bbbb01")));
		assertEquals(2, st.prefixOf(ByteString.of("ccc01")));
		assertEquals(2, st.prefixOf(ByteString.of("cccc01")));
		assertEquals(2, st.prefixOf(ByteString.of("ddd01")));
		assertEquals(3, st.prefixOf(ByteString.of("dddd01")));
		assertEquals(4, st.prefixOf(ByteString.of("eee01")));
		assertEquals(4, st.prefixOf(ByteString.of("eeee01")));
		assertEquals(4, st.prefixOf(ByteString.of("fff01")));
		assertEquals(5, st.prefixOf(ByteString.of("ffff01")));
		assertEquals(6, st.prefixOf(ByteString.of("ggg01")));
		assertEquals(6, st.prefixOf(ByteString.of("gggg01")));
	}

	public ByteString prefix1(int pref, String end) {
		ReplazableString rs = new ReplazableString();
		rs.append((byte) pref);
		rs.append(end);
		return rs;
	}

	@Test
	public void prefix1Test() {
		assertEqualsBS(ByteString.of("abcd"), prefix1('a', "bcd"));
		assertEqualsBS(ByteString.of("bcde"), prefix1('b', "cde"));
		assertEqualsBS(ByteString.of("cdef"), prefix1('c', "def"));
		assertEqualsBS(ByteString.of("defg"), prefix1('d', "efg"));
	}

	@Test
	public void mapTest() {
		String cfg = String.join(";", "bbbb", "dddd", "ffff");

		PrefixesStorage st = new PrefixesStorage();
		st.loadConfig(cfg);

		assertEqualsBS(prefix1(0, "aaaa01"), st.map(ByteString.of("aaaa01")));
		assertEqualsBS(prefix1(1, "01"), st.map(ByteString.of("bbbb01")));
		assertEqualsBS(prefix1(2, "cccc01"), st.map(ByteString.of("cccc01")));
		assertEqualsBS(prefix1(3, "01"), st.map(ByteString.of("dddd01")));

		assertEqualsBS("aaaa01", st.unmap(st.map(ByteString.of("aaaa01"))));
		assertEqualsBS("bbbb01", st.unmap(st.map(ByteString.of("bbbb01"))));
		assertEqualsBS("cccc01", st.unmap(st.map(ByteString.of("cccc01"))));
		assertEqualsBS("dddd01", st.unmap(st.map(ByteString.of("dddd01"))));

	}

	@Test
	public void serialTest() throws IOException {
		ProgressListener pl = ProgressListener.ignore();

		for (int val : new int[] { 14, 127, 129, 0xFFFF, 0x78FF }) {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			PrefixesStorage st = new PrefixesStorage();
			st.loadConfig(IntStream.range(0, val).mapToObj(String::valueOf).collect(Collectors.joining(";")));

			st.save(bos, pl);

			PrefixesStorage lo = new PrefixesStorage();
			lo.load(new ByteArrayInputStream(bos.toByteArray()), pl);
			assertEquals(st, lo);
		}
	}

	@Test
	public void serialStrTest() {
		for (int val : new int[] { 14, 127, 129, 0xFFFF, 0x78FF }) {
			PrefixesStorage st = new PrefixesStorage();
			String cfg = IntStream.range(0, val).mapToObj(String::valueOf).sorted().collect(Collectors.joining(";"));
			st.loadConfig(cfg);

			PrefixesStorage lo = new PrefixesStorage();
			String cfgac = st.saveConfig();
			assertEquals(cfg, cfgac);
			lo.loadConfig(cfgac);
			assertEquals(st, lo);
		}
	}
}
