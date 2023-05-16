/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * listener/ProgressListener.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License,
 * or (at your option) any later version. This library is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.listener;

/**
 * Interface for notifying the progress of an operation.
 *
 * @author mario.arias
 */
public interface ProgressListener {
	/**
	 * empty progress listener
	 *
	 * @return progress listener
	 */
	static ProgressListener ignore() {
		return new ProgressListener() {
			@Override
			public void notifyProgress(float level, String message) {
			}

			@Override
			public ProgressListener combine(ProgressListener listener) {
				if (listener == null) {
					return this;
				}
				return listener;
			}
		};
	}

	/**
	 * @return progress listener returning to sdtout
	 */
	static ProgressListener sout() {
		return ((level, message) -> System.out.println(level + " - " + message));
	}

	/**
	 * progress listener of a nullable listener
	 *
	 * @param listener listener
	 * @return listener or ignore listener
	 */
	static ProgressListener ofNullable(ProgressListener listener) {
		return listener == null ? ignore() : listener;
	}

	/**
	 * Send progress notification
	 *
	 * @param level   percent of the task accomplished
	 * @param message Description of the operation
	 */
	void notifyProgress(float level, String message);

	/**
	 * combine a listener with another one into a new listener
	 *
	 * @param listener the listener
	 * @return new listener
	 */
	default ProgressListener combine(ProgressListener listener) {
		if (listener == null) {
			return this;
		}
		return ((level, message) -> {
			ProgressListener.this.notifyProgress(level, message);
			listener.notifyProgress(level, message);
		});
	}
}
