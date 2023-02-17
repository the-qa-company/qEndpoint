package com.the_qa_company.qendpoint.core.util.string;

import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;

import java.util.Comparator;
import java.util.Iterator;

public class HDTEnsureDictionaryOrder {

	private static final Comparator<CharSequence> comparator = CharSequenceComparator.getInstance();

	private static void print(byte[] arr) {
		for (int i = 0; i < arr.length; i++) {
			byte b = arr[i];
			System.out.print(String.format("%02X ", b));
		}
		System.out.println();
	}

	private static void print(CharSequence seq) {
		if (seq instanceof CompactString) {
			CompactString cs1 = (CompactString) seq;
			print(cs1.data);
		}

		if (seq instanceof String) {
			String rs1 = (String) seq;
			print(rs1.getBytes());
		}
	}

	public static void check(Iterator<? extends CharSequence> it) {
		CharSequence lastCharseq = null;
		String lastStr = null;
		int cmp = 0, cmp2 = 0;
		while (it.hasNext()) {
			CharSequence charSeq = it.next();
			String str = charSeq.toString();

			if (lastCharseq != null && ((cmp = comparator.compare(lastCharseq, charSeq)) > 0)) {
				System.out.println("ERRA: " + lastCharseq + " / " + charSeq);
			}

			if (lastStr != null && ((cmp2 = lastStr.compareTo(str)) > 0)) {
				System.out.println("ERRB: " + lastStr + " / " + str);
			}

			if (Math.signum(cmp) != Math.signum(cmp2)) {
				System.out.println("Not equal: " + cmp + " / " + cmp2);
				print(lastCharseq);
				print(charSeq);
				print(lastStr);
				print(str);
			}

			lastCharseq = charSeq;
			lastStr = str;
		}
	}

	public static void main(String[] args) throws Throwable {
		HDT hdt = HDTManager.mapHDT(args[0]);

		check(hdt.getDictionary().getSubjects().getSortedEntries());
		check(hdt.getDictionary().getPredicates().getSortedEntries());
		check(hdt.getDictionary().getObjects().getSortedEntries());
		check(hdt.getDictionary().getShared().getSortedEntries());
	}
}
