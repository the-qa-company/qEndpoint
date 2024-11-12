package com.the_qa_company.qendpoint.core.util;

public class CommonUtils {
	public static int minArg(int[] array) {
		if (array.length < 2) {
			return 0;
		}
		int minIdx = 0;
		int minVal = array[0];
		for (int i = 1; i < array.length; i++) {
			if (array[i] < minVal) {
				minVal = array[i];
				minIdx = i;
			}
		}

		return minIdx;
	}
	public static int maxArg(int[] array) {
		if (array.length < 2) {
			return 0;
		}
		int maxIdx = 0;
		int maxVal = array[0];
		for (int i = 1; i < array.length; i++) {
			if (array[i] > maxVal) {
				maxVal = array[i];
				maxIdx = i;
			}
		}

		return maxIdx;
	}
	private CommonUtils() {
		throw new RuntimeException();
	};
}
