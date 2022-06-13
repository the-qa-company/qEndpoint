package com.the_qa_company.qendpoint.client;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.swing.JFrame;
import java.awt.Desktop;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class QEndpointClient extends JFrame {
	private static final Logger logger = LoggerFactory.getLogger(QEndpointClient.class);
	private static final String ICON = "icon.png";

	public QEndpointClient() {
		try {
			URL iconUrl = QEndpointClient.class.getClassLoader().getResource(ICON);
			if (iconUrl == null) {
				throw new IOException("Can't find resource " + ICON);
			}
			BufferedImage img = ImageIO.read(iconUrl);
			setIconImage(img);
		} catch (IOException io) {
			logger.warn("Can't read icon file {}", ICON, io);
		}
	}

	/**
	 * open an uri
	 *
	 * @param uri the uri
	 */
	public void openUri(URI uri) {
		if (!Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
			try {
				Runtime rt = Runtime.getRuntime();

				if (SystemUtils.IS_OS_WINDOWS) {
					rt.exec("rundll32 url.dll,FileProtocolHandler " + uri);
				} else if (SystemUtils.IS_OS_MAC_OSX) {
					rt.exec("open " + uri);
				} else if (SystemUtils.IS_OS_LINUX) {
					rt.exec("xdg-open " + uri);
				} else {
					logger.warn("Java Desktop class not supported on this platform. Please open {} in your browser",
							uri);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			try {
				Desktop.getDesktop().browse(uri);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public String getApplicationDirectory() {
		String prop;
		if (SystemUtils.IS_OS_WINDOWS) {
			prop = System.getProperty("AppData");
		} else if (SystemUtils.IS_OS_MAC_OSX) {
			prop = SystemUtils.USER_HOME == null ? null
					: (SystemUtils.USER_HOME + "/Library/Application Support/qendpoint");
		} else if (SystemUtils.IS_OS_LINUX) {
			prop = SystemUtils.USER_HOME;
		} else {
			prop = null;
		}

		if (prop == null) {
			throw new IllegalArgumentException("Platform not supported! " + SystemUtils.OS_NAME);
		}

		return prop;
	}
}
