package com.the_qa_company.qendpoint.core.util.listener;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;

public class MultiThreadListenerConsole implements MultiThreadListener {
	private static final int BAR_SIZE = 10;
	private static final String ERASE_LINE = "\r\033[K";

	private static String goBackNLine(int line) {
		return "\033[" + line + "A";
	}

	/**
	 * true if the system allow ascii sequence, false otherwise
	 */
	private static final boolean ALLOW_ASCII_SEQUENCE;

	/**
	 * true if the system allow color sequence, false otherwise
	 */
	static final boolean ALLOW_COLOR_SEQUENCE;

	static {
		String env;
		try {
			env = System.getenv("TERM");
		} catch (SecurityException e) {
			env = null;
		}

		ALLOW_ASCII_SEQUENCE = System.console() != null && !(env == null || env.isEmpty());

		String envC;
		try {
			envC = System.getenv("RDFHDT_COLOR");
		} catch (SecurityException e) {
			envC = null;
		}

		ALLOW_COLOR_SEQUENCE = System.console() != null && "true".equalsIgnoreCase(envC);
	}

	private final Map<String, String> threadMessages;
	private final boolean color;
	private int previous;

	public MultiThreadListenerConsole(boolean color) {
		this(color, ALLOW_ASCII_SEQUENCE);
	}

	public MultiThreadListenerConsole(boolean color, boolean asciiListener) {
		this.color = color || ALLOW_COLOR_SEQUENCE;
//		if (asciiListener) {
		threadMessages = new TreeMap<>();
//		} else {
//			threadMessages = null;
//		}
	}

	public String color(int r, int g, int b) {
		if (!color) {
			return "";
		}
		int color = 16 + 36 * r + 6 * g + b;
		return "\033[38;5;" + color + "m";
	}

	public String backColor(int r, int g, int b) {
		if (!color) {
			return "";
		}
		int color = 16 + 36 * r + 6 * g + b;
		return "\033[48;5;" + color + "m";
	}

	public String progressBar(float level) {
		String colorBar;
		String colorText;
		int iv = Math.min(100, Math.max(0, (int) (level)));
		if (!color) {
			colorText = "";
			colorBar = "";
		} else {
			int diff = (iv - 1) % 50 + 1;
			int delta = diff * 3 / 50;
			if (iv <= 50) {
				colorText = color(5 - delta, delta * 2 / 3, 0);
				colorBar = backColor(5 - delta, delta * 2 / 3, 0) + colorText;
			} else {
				colorText = color(2 - delta * 2 / 3, 2 + delta, 0);
				colorBar = backColor(2 - delta * 2 / 3, 2 + delta, 0) + colorText;
			}
		}
		int bar = iv * BAR_SIZE / 100;
		return colorReset() + "[" + colorBar + "#".repeat(bar) + colorReset() + " ".repeat(BAR_SIZE - bar) + "] "
				+ colorText + String.format(level >= 100 ? "%-5.1f" : "%-5.2f", level);
	}

	public String colorReset() {
		return color ? "\033[0m" : "";
	}

	public String colorThread() {
		return color(3, 1, 5);
	}

	public String colorPercentage() {
		return color(5, 1, 0);
	}

	@Override
	public synchronized void unregisterAllThreads() {
		if (threadMessages == null) {
			return;
		}
		threadMessages.clear();
		notifyProgress(0, "-");
	}

	@Override
	public synchronized void registerThread(String threadName) {
		notifyProgress(threadName, 0, "-");
	}

	@Override
	public synchronized void unregisterThread(String threadName) {
		if (threadMessages == null) {
			return;
		}
		threadMessages.remove(threadName);
		threadMessages.put("debug", "size: " + threadMessages.size());
		render();
	}

	@Override
	public synchronized void notifyProgress(String thread, float level, String message) {
		String msg = colorReset() + progressBar(level) + colorReset() + " " + message;
		if (threadMessages != null) {
			threadMessages.put(thread, msg);
//			render();
		} else {
			System.out.println(colorReset() + "[" + colorThread() + thread + colorReset() + "]" + msg);
		}
	}

	public synchronized void printLine(String line) {
		render(line);
	}

	public void removeLast() {
		StringBuilder message = new StringBuilder();
		if (previous != 0) {
			for (int i = 0; i < previous; i++) {
				message.append(goBackNLine(1)).append(ERASE_LINE);
			}
		}
		System.out.print(message);
	}

	{

		Thread thread = new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(500);
					render();
				} catch (InterruptedException e) {
					break;
				}
			}
		});

		thread.setDaemon(true);
		thread.setName("MultiThreadListenerConsole");
		thread.start();

	}

	private void render() {
		render(null);
	}

	synchronized private void render(String ln) {
		if (threadMessages == null) {
			return;
		}
		StringBuilder message = new StringBuilder();
		int lines = threadMessages.size();
		message.append("\r");
		// go back each line of the thread message

		if (previous != 0) {
			for (int i = 0; i < previous; i++) {
				message.append(goBackNLine(1)).append(ERASE_LINE);
			}
		}

		if (ln != null) {
			message.append(ln).append("\n");
		}

		int maxThreadNameSize = threadMessages.keySet().stream().mapToInt(String::length).max().orElse(0) + 1;

		// write each thread logs
		threadMessages.forEach((thread, msg) -> message.append('\r').append(colorReset()).append("[")
				.append(colorThread()).append(thread).append(colorReset()).append("]").append(" ")
				.append(".".repeat(maxThreadNameSize - thread.length())).append(" ").append(msg).append("\n"));
		// remove previous printing
		int toRemove = previous - lines;
		if (toRemove > 0) {
			message.append((ERASE_LINE + "\n").repeat(toRemove)).append(goBackNLine(toRemove));
		}
		previous = lines;

		System.out.print(message);
		System.out.flush();
	}
}
