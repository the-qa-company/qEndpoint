package com.the_qa_company.qendpoint.core.util.string;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PrefixesStorage {
	private final ReplazableString prefixStr = new ReplazableString(4);
	private final List<ByteString> prefixes = new ArrayList<>();
	private int sizeof;

	public void clear() {
		sizeof = 0;
		prefixes.clear();
	}

	public String saveConfig() {
		return String.join(";", prefixes);
	}

	public void loadConfig(String cfg) {
		this.clear();
		String[] prefixes = Objects.requireNonNullElse(cfg, "").split(";");

		if (prefixes.length == 0 || prefixes[0].isEmpty()) {
			return;
		}

		for (String prefix : prefixes) {
			addPrefix(prefix);
		}
		commitPrefixes();
	}

	/**
	 * add a prefix to the storage, the storage should be committed after using
	 * {@link #commitPrefixes()} to keep integrity
	 *
	 * @param prefix prefix
	 */
	public void addPrefix(CharSequence prefix) {
		this.prefixes.add(ByteString.copy(prefix));
	}

	/**
	 * Commit the added prefixes
	 */
	public void commitPrefixes() {
		int maxVal = (prefixes.size() + 1) * 2 + 1;
		sizeof = (BitUtil.log2(maxVal) - 1) / 8 + 1;

		this.prefixes.sort(ByteString::compareTo);
	}

	public void load(InputStream stream, ProgressListener listener) throws IOException {
		CRCInputStream cis = new CRCInputStream(stream, new CRC32());

		sizeof = (int) VByte.decode(cis);
		int numPrefixes = (int) VByte.decode(cis);

		prefixes.clear();
		for (int i = 0; i < numPrefixes; i++) {
			prefixes.add(new CompactString(IOUtil.readSizedBuffer(cis, listener)));
		}

		if (!cis.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Bitmap64 header.");
		}
	}

	public void save(OutputStream stream, ProgressListener listener) throws IOException {
		CRCOutputStream cos = new CRCOutputStream(stream, new CRC32());

		VByte.encode(cos, sizeof);
		VByte.encode(cos, prefixes.size());

		for (ByteString prefix : prefixes) {
			IOUtil.writeSizedBuffer(cos, prefix, listener);
		}

		cos.writeCRC();
	}

	private static int comparePrefix(ByteString prefix, CharSequence bs) {
		if (prefix.length() > bs.length()) {
			return prefix.compareTo(bs);
		}
		int n = prefix.length();
		int k = 0;
		while (k < n) {
			char c1 = prefix.charAt(k);
			char c2 = bs.charAt(k);
			if (c1 != c2) {
				return c1 - c2;
			}
			k++;
		}
		return 0; // same prefix
	}

	public int prefixOf(CharSequence bs) {
		if (sizeof == 0)
			return 0;

		int min = 0, max = prefixes.size(), mid;

		while (min < max) {
			mid = (min + max) / 2;

			int cmp = comparePrefix(prefixes.get(mid), bs);
			if (cmp == 0) {
				return (mid << 1) | 1;
			}
			if (cmp > 0) {
				max = mid;
			} else {
				min = mid + 1;
			}
		}

		return (min << 1);

	}

	public ByteString map(ByteString bs) {
		if (bs == null || sizeof == 0)
			return bs;
		int prefix = prefixOf(bs);
		int wprefix = prefix;

		prefixStr.clear();
		for (int i = 0; i < sizeof; i++) {
			prefixStr.append((byte) (wprefix & 0xFF));
			wprefix >>>= 8;
		}

		if ((prefix & 1) == 0) {
			return prefixStr.copyAppend(bs); // nothing to remove
		}
		return prefixStr.copyAppend(bs, prefixes.get(prefix >> 1).length());
	}

	public ByteString unmap(ByteString bs) {
		if (sizeof == 0)
			return bs;

		int prefix = bs.charAt(0);

		if ((prefix & 1) == 0) {
			return bs.subSequence(sizeof); // not a prefixed data
		}

		for (int i = 1; i < sizeof; i++) {
			prefix |= (bs.charAt(i)) << (i << 3);
		}

		prefix >>= 1; // remove prefix bit
		return prefixes.get(prefix).copyAppend(bs, sizeof);
	}

	public void map(TripleString ts) {
		ts.setSubject(map(ByteString.of(ts.getSubject())));
		ts.setPredicate(map(ByteString.of(ts.getPredicate())));
		ts.setObject(map(ByteString.of(ts.getObject())));
		ts.setGraph(map(ByteString.of(ts.getGraph())));
	}

	public void unmap(TripleString ts) {
		ts.setSubject(unmap(ByteString.of(ts.getSubject())));
		ts.setPredicate(unmap(ByteString.of(ts.getPredicate())));
		ts.setObject(unmap(ByteString.of(ts.getObject())));
		ts.setGraph(unmap(ByteString.of(ts.getGraph())));
	}

	public List<ByteString> getPrefixes() {
		return prefixes;
	}

	public int sizeof() {
		return sizeof;
	}

	public boolean empty() {
		return sizeof == 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof PrefixesStorage ps) || ps.sizeof != sizeof || ps.prefixes.size() != prefixes.size()) {
			return false;
		}

		for (int i = 0; i < prefixes.size(); i++) {
			if (prefixes.get(i).compareTo(ps.prefixes.get(i)) != 0)
				return false;
		}

		return true;
	}

	public void dump() {
		System.out.println("prefixes (" + prefixes.size() + ")");
		prefixes.forEach(p -> System.out.println("- " + p));
	}

	@Override
	public String toString() {
		return "PrefixStorage{" + sizeof + "," + prefixes.size() + "}";
	}

	public ByteString getPrefix(int prefix) {
		if ((prefix & 1) == 0) {
			return ByteString.empty();
		}
		return prefixes.get(prefix >>> 1);
	}

	public PrefixesStorage copy() {
		PrefixesStorage ps = new PrefixesStorage();
		ps.loadConfig(saveConfig());
		return ps;
	}
}
