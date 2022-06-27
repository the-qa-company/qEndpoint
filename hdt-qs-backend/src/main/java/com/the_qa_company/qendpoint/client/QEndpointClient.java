package com.the_qa_company.qendpoint.client;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.swing.JFrame;
import java.awt.Desktop;
import java.awt.image.BufferedImage;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class QEndpointClient extends JFrame {
	private static final Logger logger = LoggerFactory.getLogger(QEndpointClient.class);
	private static final String ICON = "icon.png";
	private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmss");

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

		Path logDir = this.applicationDirectory.resolve("logs");
		Files.createDirectories(logDir);

		Path out = backupIfExists(logDir.resolve("log.out"));
		Path err = backupIfExists(logDir.resolve("log.err"));

		FileOutputStream f1 = null;
		FileOutputStream f2 = null;

		try {
			f1 = new FileOutputStream(out.toFile());
			f2 = new FileOutputStream(err.toFile());

			System.setOut(new PrintStream(SplitStream.of(f1, System.out)));
			System.setErr(new PrintStream(SplitStream.of(f2, System.err)));
		} catch (Exception e) {
			logger.warn("Can't redirect streams", e);
			try {
				try {
					if (f1 != null) {
						f1.close();
					}
				} finally {
					if (f2 != null) {
						f2.close();
					}
				}
			} catch (Exception ee) {
				// ignore close error
			}
		}
	}

	private static Path backupIfExists(Path p) throws IOException {
		if (Files.exists(p)) {
			Path old = p.resolveSibling("old");
			Files.createDirectories(old);
			Path next = old.resolve(p.getFileName() + "_" + FORMAT.format(Calendar.getInstance().getTime()));
			logger.info("move old file to {}", next);
			Files.move(p, next);
		}
		return p;
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
