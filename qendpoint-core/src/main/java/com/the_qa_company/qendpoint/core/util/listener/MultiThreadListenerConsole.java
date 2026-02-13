package com.the_qa_company.qendpoint.core.util.listener;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.listener.ProgressMessage;
import com.the_qa_company.qendpoint.core.util.ProfilingUtil;
import com.sun.management.GarbageCollectionNotificationInfo;

public class MultiThreadListenerConsole implements MultiThreadListener {
	private static final int BAR_SIZE = 10;
	private static final String ERASE_LINE = "\r\033[K";
	private static final long REFRESH_MILLIS = 300;
	private static final long GC_WINDOW_MILLIS = 10_000L;
	private static final GcMonitor GC_MONITOR = new GcMonitor();

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

	private final Map<String, ThreadState> threadMessages;
	private final boolean color;
	private final long startNanos;
	private int previous;

	private static final class ThreadState {
		private float level;
		private ProgressMessage message;
	}

	private static final class MemorySnapshot {
		private final long used;
		private final long total;

		private MemorySnapshot(long used, long total) {
			this.used = used;
			this.total = total;
		}

		private static MemorySnapshot capture() {
			Runtime runtime = Runtime.getRuntime();
			long used = runtime.totalMemory() - runtime.freeMemory();
			long total = runtime.maxMemory();
			return new MemorySnapshot(used, total);
		}

		private String format() {
			return ProfilingUtil.tidyFileSize(used) + "/" + ProfilingUtil.tidyFileSize(total);
		}
	}

	private static final class GcSample {
		private final long timestampMillis;
		private final long durationMillis;

		private GcSample(long timestampMillis, long durationMillis) {
			this.timestampMillis = timestampMillis;
			this.durationMillis = durationMillis;
		}
	}

	private static final class GcMonitor {
		private final Deque<GcSample> samples = new ArrayDeque<>();
		private final List<NotificationEmitter> emitters = new ArrayList<>();
		private final NotificationListener listener = this::handleNotification;
		private final boolean supported;

		private GcMonitor() {
			supported = register();
		}

		private boolean register() {
			boolean registered = false;
			for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
				if (!(bean instanceof NotificationEmitter)) {
					continue;
				}
				try {
					NotificationEmitter emitter = (NotificationEmitter) bean;
					emitter.addNotificationListener(listener, null, null);
					emitters.add(emitter);
					registered = true;
				} catch (RuntimeException ex) {
					// Ignore listener registration failures.
				}
			}
			return registered;
		}

		private void handleNotification(Notification notification, Object handback) {
			if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
				return;
			}
			Object data = notification.getUserData();
			if (!(data instanceof CompositeData)) {
				return;
			}
			try {
				GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) data);
				record(info.getGcInfo().getDuration());
			} catch (RuntimeException ex) {
				// Ignore malformed notifications.
			}
		}

		private void record(long durationMillis) {
			long now = System.currentTimeMillis();
			synchronized (samples) {
				samples.addLast(new GcSample(now, durationMillis));
				trim(now);
			}
		}

		private void trim(long now) {
			long cutoff = now - GC_WINDOW_MILLIS;
			while (!samples.isEmpty() && samples.peekFirst().timestampMillis < cutoff) {
				samples.removeFirst();
			}
		}

		private String formatPercentLastMinute() {
			if (!supported) {
				return "n/a";
			}
			long now = System.currentTimeMillis();
			long total = 0L;
			synchronized (samples) {
				trim(now);
				for (GcSample sample : samples) {
					total += sample.durationMillis;
				}
			}
			double percent = Math.min(100.0, (total * 100.0) / GC_WINDOW_MILLIS);
			return String.format(Locale.ROOT, "%.2f%%", percent);
		}
	}

	public MultiThreadListenerConsole(boolean color) {
		this(color, ALLOW_ASCII_SEQUENCE);
	}

	public MultiThreadListenerConsole(boolean color, boolean asciiListener) {
		this.color = color || ALLOW_COLOR_SEQUENCE;
		threadMessages = new TreeMap<>();
		startNanos = System.nanoTime();
		startRenderThread();
	}

	private void startRenderThread() {
		Thread thread = new Thread(() -> {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					Thread.sleep(REFRESH_MILLIS);
					render();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		});
		thread.setDaemon(true);
		thread.setName("MultiThreadListenerConsole");
		thread.start();
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
		notifyProgress("debug", 0, "size: {}", threadMessages.size());
		render();
	}

	@Override
	public void notifyProgress(String thread, float level, String message) {
		notifyProgress(thread, level, ProgressMessage.literal(message));
	}

	public void notifyProgress(String thread, float level, ProgressMessage message) {
		synchronized (this) {
			if (threadMessages != null) {
				ThreadState state = threadMessages.get(thread);
				if (state == null) {
					state = new ThreadState();
					threadMessages.put(thread, state);
				}
				state.level = level;
				state.message = message;
			} else {
				String msg = colorReset() + progressBar(level) + colorReset() + " " + message.render();
				System.out.println(colorReset() + "[" + colorThread() + thread + colorReset() + "]" + msg);
			}
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

	private void render() {
		render(null);
	}

	synchronized private void render(String ln) {
		if (threadMessages == null) {
			return;
		}
		StringBuilder message = new StringBuilder();
		int lines = threadMessages.size() + 1;
		message.append("\r");
		if (previous != 0) {
			for (int i = 0; i < previous; i++) {
				message.append(goBackNLine(1)).append(ERASE_LINE);
			}
		}
		if (ln != null) {
			message.append(ln).append("\n");
		}

		int maxThreadNameSize = threadMessages.keySet().stream().mapToInt(String::length).max().orElse(0) + 1;

		threadMessages.forEach((thread, state) -> message.append('\r').append(colorReset()).append("[")
				.append(colorThread()).append(thread).append(colorReset()).append("]").append(" ")
				.append(".".repeat(maxThreadNameSize - thread.length())).append(" ").append(renderState(state))
				.append("\n"));
		message.append(renderStatsLine()).append("\n");
		int toRemove = previous - lines;
		if (toRemove > 0) {
			message.append((ERASE_LINE + "\n").repeat(toRemove)).append(goBackNLine(toRemove));
		}
		previous = lines;

		System.out.print(message);
		System.out.flush();
	}

	private String renderState(ThreadState state) {
		String message = state.message == null ? "" : state.message.render();
		return colorReset() + progressBar(state.level) + colorReset() + " " + message;
	}

	private String renderStatsLine() {
		MemorySnapshot memory = MemorySnapshot.capture();
		String gcPercent = GC_MONITOR.formatPercentLastMinute();
		String uptime = formatUptime();
		return colorReset() + "[" + colorThread() + "stats" + colorReset() + "] mem used/total: " + memory.format()
				+ " | GC: " + gcPercent + " | up: " + uptime;
	}

	private String formatUptime() {
		long elapsedNanos = System.nanoTime() - startNanos;
		long elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(elapsedNanos);
		long hours = elapsedSeconds / 3600;
		long minutes = (elapsedSeconds % 3600) / 60;
		long seconds = elapsedSeconds % 60;
		return String.format(Locale.ROOT, "%02d:%02d:%02d", hours, minutes, seconds);
	}
}
