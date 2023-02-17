/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/
 * ProfilingUtil.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
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
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.util;

import com.sun.management.HotSpotDiagnosticMXBean;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

/**
 * @author mario.arias
 */
@SuppressWarnings("restriction")
public class ProfilingUtil {
	// This is the name of the HotSpot Diagnostic MBean
	private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

	// field to store the hotspot diagnostic MBean
	private static volatile HotSpotDiagnosticMXBean hotspotMBean;

	private ProfilingUtil() {
	}

	/**
	 * Call this method from your application whenever you want to dump the heap
	 * snapshot into a file.
	 *
	 * @param fileName name of the heap dump file
	 * @param live     flag that tells whether to dump only the live objects
	 */
	public static void dumpHeap(String fileName, boolean live) {
		// initialize hotspot diagnostic MBean
		initHotspotMBean();
		try {
			hotspotMBean.dumpHeap(fileName, live);
		} catch (RuntimeException re) {
			throw re;
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}
	}

	// initialize the hotspot diagnostic MBean field
	private static void initHotspotMBean() {
		if (hotspotMBean == null) {
			synchronized (ProfilingUtil.class) {
				if (hotspotMBean == null) {
					hotspotMBean = getHotspotMBean();
				}
			}
		}
	}

	// get the hotspot diagnostic MBean from the platform MBean server
	private static HotSpotDiagnosticMXBean getHotspotMBean() {
		try {
			MBeanServer server = ManagementFactory.getPlatformMBeanServer();
			HotSpotDiagnosticMXBean bean = ManagementFactory.newPlatformMXBeanProxy(server, HOTSPOT_BEAN_NAME,
					HotSpotDiagnosticMXBean.class);
			return bean;
		} catch (RuntimeException re) {
			throw re;
		} catch (Exception exp) {
			throw new RuntimeException(exp);
		}
	}

	public static String tidyFileSize(long size) {
		long calcSize;
		String str;
		if (size >= 1024 * 1024 * 1024) {
			calcSize = (long) (((double) size) / (1024 * 1024 * 1024));
			str = calcSize + "GB";
		} else if (size >= 1024 * 1024) {
			calcSize = (long) (((double) size) / (1024 * 1024));
			str = calcSize + "MB";
		} else if (size >= 1024) {
			calcSize = (long) (((double) size) / (1024));
			str = calcSize + "KB";
		} else {
			calcSize = size;
			str = calcSize + "B";
		}
		return str;
	}

	/**
	 * A method for getting a property denoting a size in bytes (like size of
	 * cache for example) If the property is not set the method returns -1, else
	 * it checks if it ends with a k, K, m, M, g or G and multiplies the number
	 * before with the appropriate power of 2 before returning it.
	 */
	public static long parseSize(String property) {
		if (property == null || property.equals(""))
			return -1;

		property = property.trim();

		char lastChar = property.charAt(property.length() - 1);

		switch (lastChar) {
		case 'k':
		case 'K':
			return Long.parseLong(property.substring(0, property.length() - 1)) * 1024;
		case 'm':
		case 'M':
			return Long.parseLong(property.substring(0, property.length() - 1)) * 1024 * 1024;
		case 'g':
		case 'G':
			return Long.parseLong(property.substring(0, property.length() - 1)) * 1024 * 1024 * 1024;
		default:
			return Long.parseLong(property);
		}
	}

	public static void showMemory(String label) {
		System.out.println(label + ": " + getMemory());

	}

	public static String getMemory() {
		return tidyFileSize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " / "
				+ tidyFileSize(Runtime.getRuntime().totalMemory()) + " / "
				+ tidyFileSize(Runtime.getRuntime().maxMemory());
	}
}
