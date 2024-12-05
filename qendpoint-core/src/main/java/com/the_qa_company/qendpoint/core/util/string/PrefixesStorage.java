package com.the_qa_company.qendpoint.core.util.string;

import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.BitUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PrefixesStorage {
    private final ReplazableString prefixStr = new ReplazableString(4);
    private final List<ByteString> prefixes = new ArrayList<>();
    private int sizeof;


    public void loadConfig(String cfg) {
        this.prefixes.clear();
        String[] prefixes = Objects.requireNonNullElse(cfg, "").split(";");

        if (prefixes.length == 0 || prefixes[0].isEmpty()) {
            sizeof = 0;
            return;
        }

        int maxVal = prefixes.length * 2 + 1;

        sizeof = (BitUtil.log2(maxVal) - 1) / 8 + 1;

        for (String prefix : prefixes) {
            this.prefixes.add(ByteString.of(prefix));
        }
    }

    private static int comparePrefix(ByteString prefix, ByteString bs) {
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

    public int prefixOf(ByteString bs) {
        if (sizeof == 0) return 0;

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
        if (bs == null || sizeof == 0) return bs;
        int prefix = prefixOf(bs);
        int wprefix = prefix;

        prefixStr.clear();
        byte[] buff = prefixStr.getBuffer();
        for (int i = 0; i < sizeof; i++) {
            prefixStr.append((byte)(wprefix & 0xFF));
            wprefix >>>= 8;
        }

        if ((prefix & 1) == 0) {
            return prefixStr.copyAppend(bs); // nothing to remove
        }
        return prefixStr.copyAppend(bs, prefixes.get(prefix >> 1).length());
    }

    public ByteString unmap(ByteString bs) {
        if (sizeof == 0) return bs;

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
}
