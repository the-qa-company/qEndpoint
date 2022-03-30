package com.the_qa_company.q_endpoint.utils;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * a worker to
 * @param <T>
 */
public class TreeWorker<T> {
	private static final AtomicInteger JOB_ID_NAME = new AtomicInteger();
	private final TreeWorkerCat<T> catFunction;
	private final Supplier<T> baseLevelSupplier;
	private final Consumer<T> delete;
	private int maxLevel = 0;
	private Worker fetchJob = null;
	private final List<Element> elements = new ArrayList<>();
	private final Worker[] workers;
	private boolean done = false;
	private TreeWorkerException throwable;

	public TreeWorker(TreeWorkerCat<T> catFunction, Supplier<T> baseLevelSupplier, Consumer<T> delete) throws TreeWorkerException {
		this(catFunction, baseLevelSupplier, delete, Runtime.getRuntime().availableProcessors());
	}

	public TreeWorker(TreeWorkerCat<T> catFunction, Supplier<T> baseLevelSupplier, Consumer<T> delete, int workers) throws TreeWorkerException {
		this.catFunction = catFunction;
		this.baseLevelSupplier = baseLevelSupplier;
		this.delete = delete;
		if (workers <= 0) {
			throw new IllegalArgumentException("worker count can't be <= 0!");
		}
		T t = baseLevelSupplier.get();
		if (t == null) {
			throw new TreeWorkerException("no base element!");
		}
		elements.add(new Element(t, 0));
		synchronized (elements) {
			this.workers = new TreeWorker.Worker[workers];
			for (int i = 0; i < this.workers.length; i++) {
				this.workers[i] = new Worker();
				this.workers[i].start();
			}
		}
	}

	private void clearData() {
		for (Element e: elements) {
			delete.accept(e.t);
		}
	}

	public T waitToComplete() throws TreeWorkerException {
		try {
			for (Worker w: workers) {
				w.join();
			}
		} catch (InterruptedException e) {
			clearData();
			throw new TreeWorkerException(e);
		}

		if (throwable != null) {
			clearData();
			throw throwable;
		}

		if (!done) {
			clearData();
			// shouldn't be possible?
			throw new TreeWorkerException("The worker isn't done!");
		}
		if (elements.isEmpty()) {
			return null;
		}
		return elements.get(0).t;
	}

	private TreeWorkerJob getJob(Worker worker) throws InterruptedException {
		while (true) {
			synchronized (elements) {
				if (done) {
					if (elements.size() == 1) {
						return null; // end, no ascend/merge required
					}
					Tuple w = searchDir(0, 1, 1);
					if (w == null) {
						return null; // end
					} else {
						w.remove();
						if (w.size() == 1) {
							return new Ascend(w.a);
						} else { //size == 2
							return new Merge(w.a, w.b);
						}
					}
				} else {
					Tuple w = searchDir(maxLevel, -1, 2);
					if (w == null) {
						if (fetchJob == null) {
							fetchJob = worker;
							return new Fetch();
						} else {
							elements.wait();
							if (throwable != null) {
								return null; // end before waiting
							}
						}
					} else {
						w.remove();
						return new Merge(w.a, w.b);
					}
				}
			}
		}
	}

	public Tuple searchDir(int start, int direction, int min) {
		if (direction < 0) {
			for (int i = start; i >= 0; i--) {
				Tuple tuple = searchAtLevel(i);
				if (tuple.size() >= min) {
					return tuple;
				}
			}
		} else {
			for (int i = start; i <= maxLevel; i++) {
				Tuple tuple = searchAtLevel(i);
				if (tuple.size() >= min) {
					return tuple;
				}
			}
		}
		return null;
	}

	public Tuple searchAtLevel(int level) {
		synchronized (elements) {
			Element old = null;
			for (Element e: elements) {
				if (e.level == level) {
					if (old != null) {
						return new Tuple(old, e);
					} else {
						old = e;
					}
				}
			}
			return new Tuple(old, null);
		}
	}

	@FunctionalInterface
	public interface TreeWorkerCat<T> {
		T construct(T a, T b);
	}

	public boolean isCompleted() {
		synchronized (elements) {
			return (done && elements.size() == 1) || throwable != null;
		}
	}

	private class Element {
		T t;
		int level;

		public Element(T t, int level) {
			this.t = t;
			this.level = level;
		}
	}

	private class Tuple {
		Element a, b;

		public Tuple(Element a, Element b) {
			this.a = a;
			this.b = b;
		}

		public void remove() {
			if (a != null) {
				elements.remove(a);
			}
			if (b != null) {
				elements.remove(b);
			}
		}

		public int size() {
			if (a == null) {
				return 0;
			}
			if (b == null) {
				return 1;
			}
			return 2;
		}
	}

	private abstract static class TreeWorkerJob {
		abstract void runJob();
		void clear() {
		}
	}

	private class Fetch extends TreeWorkerJob {
		@Override
		public void runJob() {
			T t = baseLevelSupplier.get();
			synchronized (elements) {
				if (t == null) {
					done = true;
				} else {
					elements.add(new Element(t, 0));
				}
				fetchJob = null;
				elements.notifyAll();
			}
		}
	}

	private class Merge extends TreeWorkerJob {
		Element a, b;

		public Merge(Element a, Element b) {
			this.a = a;
			this.b = b;
			assert a.level == b.level: "cat elements from different level!";
		}

		@Override
		public void runJob() {
			T t = catFunction.construct(a.t, b.t);
			synchronized (elements) {
				elements.add(new Element(t, a.level + 1));
				maxLevel = Math.max(maxLevel, a.level + 1);
			}
		}
		@Override
		void clear() {
			delete.accept(a.t);
			delete.accept(b.t);
		}
	}

	private class Ascend extends TreeWorkerJob {
		Element e;

		public Ascend(Element e) {
			this.e = e;
		}

		@Override
		public void runJob() {
			synchronized (elements) {
				e.level++;
				elements.add(e);
				maxLevel = Math.max(maxLevel, e.level);
			}
		}
		@Override
		void clear() {
			delete.accept(e.t);
		}
	}

	private class Worker extends Thread {
		public Worker() {
			super("JobWorker#" + JOB_ID_NAME.incrementAndGet());
		}

		@Override
		public void run() {
			while (!isCompleted()) {
				TreeWorkerJob job = null;
				try {
					job = getJob(this);
					if (job != null) {
						job.runJob();
					}
				} catch (Throwable t) {
					if (job != null) {
						job.clear();
					}
					synchronized (elements) {
						throwable = new TreeWorkerException(t);
						elements.notifyAll();
					}
				}
			}
		}
	}

	private static class TreeWorkerException extends Exception {
		public TreeWorkerException(Throwable cause) {
			super(cause);
		}

		public TreeWorkerException(String message) {
			super(message);
		}
	}
}
