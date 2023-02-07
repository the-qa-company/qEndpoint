/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/
 * listener/IntermediateListener.java $ Revision: $Rev: 191 $ Last modified:
 * $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.util.listener;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;

/**
 * Provides notifications by dividing an overall task in subtasks. Call
 * setRange() with the estimated percent of the overall task that the subtask
 * will take (i.e. 20-40%). Then call the subtask providing the
 * IntermediateListener as Callback. The IntermediateListener will translate the
 * notifications of the subtask (in the 0-100 range) to the range supplied
 * (20-40) by using linear interpolation.
 *
 * @author mario.arias
 */
public class IntermediateListener implements ProgressListener {

	private final ProgressListener child;
	private float min, max;
	private String prefix;

	/**
	 * Create an IntermediateListener that translates notifications of a child
	 * into a broader range.
	 *
	 * @param child child listener
	 */
	public IntermediateListener(ProgressListener child) {
		this(child, 0, 100);
	}

	/**
	 * Create an IntermediateListener that translates notifications of a child
	 * into a broader range.
	 *
	 * @param child child listener
	 * @param min   minimum value
	 * @param max   maximum value
	 */
	public IntermediateListener(ProgressListener child, float min, float max) {
		this(child, min, max, "");
	}

	/**
	 * Create an IntermediateListener that translates notifications of a child
	 * into a broader range.
	 *
	 * @param child  child listener
	 * @param prefix prefix of this listener
	 */
	public IntermediateListener(ProgressListener child, String prefix) {
		this(child, 0, 100, prefix);
	}

	/**
	 * Create an IntermediateListener that translates notifications of a child
	 * into a broader range.
	 *
	 * @param child  child listener
	 * @param min    minimum value
	 * @param max    maximum value
	 * @param prefix prefix of this listener
	 */
	public IntermediateListener(ProgressListener child, float min, float max, String prefix) {
		this.child = child;
		this.min = min;
		this.max = max;
		this.prefix = prefix;
	}

	/**
	 * Forward a new notification adjusting the percent to the specified range.
	 */
	/*
	 * (non-Javadoc)
	 * @see hdt.ProgressListener#notifyProgress(float, java.lang.String)
	 */
	@Override
	public void notifyProgress(float level, String message) {
		if (child != null) {
			float newlevel = min + level * (max - min) / 100;
			child.notifyProgress(newlevel, prefix + message);
		}
	}

	/**
	 * Set the current range of notifications, For example if the range 20-40%
	 * is specified, when the child notifies 0, this IntermediateListener
	 * notifies the parent with 20%, and when the child notifies 100, the
	 * IntermediateListener notifies 40. Any intermediate values are linearly
	 * interpolated.
	 *
	 * @param min minimum value
	 * @param max maximum value
	 */
	public void setRange(float min, float max) {
		this.min = min;
		this.max = max;
	}

	/**
	 * Set the prefix for this listener, will be put before the messages of this
	 * listener
	 *
	 * @param prefix the prefix
	 */
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
}
