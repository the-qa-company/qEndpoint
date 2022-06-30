package com.the_qa_company.qendpoint.client;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.swing.JFrame;
import java.awt.Desktop;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

public class QEndpointClient extends JFrame {
	private static final Logger logger = LoggerFactory.getLogger(QEndpointClient.class);
	private static final String ICON = "icon.png";

	private final Path applicationDirectory;

	public QEndpointClient() throws IOException {
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

		String applicationDirectory;
		if (SystemUtils.IS_OS_WINDOWS) {
			applicationDirectory = System.getenv("APPDATA");
		} else if (SystemUtils.IS_OS_MAC_OSX) {
			applicationDirectory = SystemUtils.USER_HOME == null ? null
					: (SystemUtils.USER_HOME + "/Library/Application Support/qendpoint");
		} else if (SystemUtils.IS_OS_LINUX) {
			applicationDirectory = SystemUtils.USER_HOME;
		} else {
			applicationDirectory = null;
		}

		if (applicationDirectory == null) {
			throw new IllegalArgumentException("Platform not supported! " + SystemUtils.OS_NAME);
		}
		this.applicationDirectory = Path.of(applicationDirectory).resolve("qendpoint");
		Files.createDirectories(this.applicationDirectory);
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

	public Path getApplicationDirectory() {
		return applicationDirectory;
	}
}
