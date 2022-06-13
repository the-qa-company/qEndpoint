package com.the_qa_company.qendpoint.client;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.swing.JFrame;
import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

@Component
public class CommandLineStart implements CommandLineRunner {
	private static final Logger logger = LoggerFactory.getLogger(CommandLineStart.class);

	/**
	 * open an uri
	 *
	 * @param uri the uri
	 */
	public static void openUri(URI uri) {
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

	@Value("${server.port}")
	String port;

	@Override
	public void run(String... args) throws Exception {

		boolean client = Arrays.stream(args).anyMatch(arg -> arg.contains("--client"));

		if (client) {
			new QEndpointGui();
			openUri(new URI("http://localhost:" + port + "/"));
		} else {
			System.out.println("server mode");
		}
	}

	static class QEndpointGui extends JFrame {}
}
