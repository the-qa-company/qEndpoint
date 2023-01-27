/**
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Dennis
 * Diefenbach: dennis.diefenbach@univ-st-etienne.fr
 */

package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.util.disk.LongArrayDisk;

import java.nio.file.Path;

/**
 * Implements an index on top of the Bitmap64 to solve select and rank queries
 * more efficiently.
 * <p>
 * index -&gt; O(n) rank1 -&gt; O(1) select1 -&gt; O(log log n)
 *
 * @author mario.arias
 * @deprecated Use {@link Bitmap375Big#disk(Path, long, boolean)} instead
 */
@Deprecated
public class Bitmap375Disk extends Bitmap375Big {

	public Bitmap375Disk(String location) {
		this(location, W);
	}

	public Bitmap375Disk(Path location) {
		this(location, W);
	}

	public Bitmap375Disk(String location, long nbits) {
		this(location, nbits, false);
	}

	public Bitmap375Disk(Path location, long nbits) {
		this(location, nbits, false);
	}

	public Bitmap375Disk(String location, boolean useDiskSuperIndex) {
		this(location, W, useDiskSuperIndex);
	}

	public Bitmap375Disk(Path location, boolean useDiskSuperIndex) {
		this(location, W, useDiskSuperIndex);
	}

	public Bitmap375Disk(String location, long nbits, boolean useDiskSuperIndex) {
		this(Path.of(location), nbits, useDiskSuperIndex);
	}

	public Bitmap375Disk(Path location, long nbits, boolean useDiskSuperIndex) {
		super(new LongArrayDisk(location, numWords(nbits)), location, useDiskSuperIndex);
	}

}
