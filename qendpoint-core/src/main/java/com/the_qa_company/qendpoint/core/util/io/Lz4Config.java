/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/io
 * /Lz4Config.java $ Revision: $Rev: 0 $ Last modified: $Date: 2026-01-23
 * 12:00:00 +0000 (fri, 23 jan 2026) $ Last modified by: $Author: codex $ This
 * library is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */
package com.the_qa_company.qendpoint.core.util.io;

/**
 * Toggle LZ4 compression across the codebase.
 * <p>
 * Set to {@code false} to disable LZ4 compression and use raw data instead.
 */
public final class Lz4Config {
	public static volatile boolean ENABLED = true;

	private Lz4Config() {
	}

	public static void setEnabled(boolean enabled) {
		ENABLED = enabled;
	}

	public static boolean isEnabled() {
		return ENABLED;
	}
}
