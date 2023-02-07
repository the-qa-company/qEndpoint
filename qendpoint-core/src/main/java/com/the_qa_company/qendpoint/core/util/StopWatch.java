/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/
 * StopWatch.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
 * 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org
 */
package com.the_qa_company.qendpoint.core.util;

public class StopWatch {
	protected long ini, end;

	public StopWatch() {
		reset();
	}

	public void reset() {
		ini = end = System.nanoTime();
	}

	public void stop() {
		end = System.nanoTime();
	}

	public long getMeasure() {
		return end - ini;
	}

	public long stopAndGet() {
		stop();
		return getMeasure();
	}

	public String stopAndShow() {
		stop();
		return toString();
	}

	@Override
	public String toString() {
		return usToString(getMeasure() / 1000);
	}

	private String usToString(long us) {
		long totalSecs = us / 1000000;
		int hours = (int) (totalSecs / 3600);
		int mins = (int) (totalSecs / 60) % 60;
		int secs = (int) (totalSecs % 60);
		int ms = (int) (us % 1000000) / 1000;
		us = us % 1000;

		StringBuilder out = new StringBuilder();
		if (hours > 0) {
			out.append(hours).append(" hour ");
		}
		if (mins > 0) {
			out.append(mins).append(" min ");
		}
		if (secs > 0) {
			out.append(secs).append(" sec ");
		}
		if (ms > 0) {
			out.append(ms).append(" ms ");
		}
		if (us > 0) {
			out.append(us).append(" us ");
		}
		return out.toString().trim();
	}

}
