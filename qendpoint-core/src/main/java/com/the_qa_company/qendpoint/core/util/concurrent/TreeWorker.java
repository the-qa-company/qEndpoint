package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * a worker to parse tree operation
 *
 * @param <S> the type used in the tree to supply
 * @param <T> the type used in the tree
 * @author Antoine Willerval
 */
public class TreeWorker<S, T> {
	/**
	 * ID fetcher for the workers
	 */
	private static final AtomicInteger JOB_ID_NAME = new AtomicInteger();

	/**
	 * Sync object for the FETCH operation
	 */
	private final Object FETCH_SYNC = new Object() {};
	/**
	 * Sync object for waiting for new job
	 */
	private final Object WAITING_SYNC = new Object() {};
	/**
	 * Sync object to show the current count of workers in the ProgressListener
	 */
	private final Object WORKING_SYNC = new Object() {};
	/**
	 * Cat function (T[]) -> T
	 */
	private final TreeWorkerCat<T> catFunction;
	/**
	 * Supplier Function () -> S
	 */
	private final TreeWorkerSupplier<S> baseLevelSupplier;
	/**
	 * Map Function (S) -> T
	 */
	private final TreeWorkerMap<S, T> mapFunction;
	/**
	 * Delete Function (T) -> void
	 */
	private final TreeWorkerDelete<T> delete;
	/**
	 * Function to create array of type T
	 */
	private final IntFunction<T[]> arrayBuilder;
	/**
	 * the current maximum level of the elements
	 */
	private int maxLevel = 0;
	/**
	 * the count of workers waiting for a job
	 */
	private int workerWaiting = 0;
	/**
	 * the count of working workers
	 */
	private int workerWorking;
	/**
	 * the minimum number of elements to merge when the supplying phase isn't
	 * completed
	 */
	private final int treeCount;
	/**
	 * the mapped elements waiting for a merge (T[])
	 */
	private final List<Element> elements = new ArrayList<>();
	/**
	 * the supplied elements waiting for a map (S[])
	 */
	private final List<S> suppliedElements = new ArrayList<>();
	/**
	 * the worker threads
	 */
	private final List<Worker> workers;
	/**
	 * if the TreeWorker is started
	 */
	private boolean started = false;
	/**
	 * if the fetch phase is completed
	 */
	private boolean fetchDone = false;
	/**
	 * if the map phase is completed
	 */
	private boolean mapDone = false;
	/**
	 * any throwable returned by the TreeWorker
	 */
	private TreeWorkerException throwable;
	/**
	 * the progress listener
	 */
	private MultiThreadListener listener;

	/**
	 * create a tree worker
	 *
	 * @param catFunction       the function to cat 2 nodes
	 * @param baseLevelSupplier the supplier to get base nodes
	 * @param delete            the delete method to delete data in case of
	 *                          error, can be null if no delete is required
	 * @param arrayBuilder      method to create an array of type T
	 * @throws TreeWorkerException            if the tree worker can't be
	 *                                        created
	 * @throws java.lang.NullPointerException if catFunction or
	 *                                        baseLevelSupplier is null
	 */
	public TreeWorker(TreeWorkerCat<T> catFunction, TreeWorkerSupplier<S> baseLevelSupplier, TreeWorkerDelete<T> delete,
			TreeWorkerMap<S, T> mapFunction, IntFunction<T[]> arrayBuilder) throws TreeWorkerException {
		this(catFunction, baseLevelSupplier, delete, mapFunction, arrayBuilder,
				Runtime.getRuntime().availableProcessors(), 1);
	}

	/**
	 * create a tree worker
	 *
	 * @param workerObject the worker object
	 * @param arrayBuilder method to create an array of type T
	 * @param workers      the number of workers to use
	 * @param nodePerMerge number of simultaneous merge tree (at least 1)
	 * @throws TreeWorkerException            if the tree worker can't be
	 *                                        created
	 * @throws java.lang.NullPointerException if catFunction or
	 *                                        baseLevelSupplier is null
	 */
	public <E extends TreeWorkerCat<T> & TreeWorkerSupplier<S> & TreeWorkerDelete<T> & TreeWorkerMap<S, T>> TreeWorker(
			E workerObject, IntFunction<T[]> arrayBuilder, int workers, int nodePerMerge) throws TreeWorkerException {
		this(workerObject, workerObject, workerObject, workerObject, arrayBuilder, workers, nodePerMerge);
	}

	/**
	 * create a tree worker
	 *
	 * @param catFunction       the function to cat 2 nodes
	 * @param baseLevelSupplier the supplier to get base nodes
	 * @param delete            the delete method to delete data in case of
	 *                          error, can be null if no delete is required
	 * @param mapFunction       the map function
	 * @param arrayBuilder      method to create an array of type T
	 * @param workers           the number of workers to use
	 * @param nodePerMerge      number of simultaneous merge tree (at least 1)
	 * @throws TreeWorkerException            if the tree worker can't be
	 *                                        created
	 * @throws java.lang.NullPointerException if catFunction or
	 *                                        baseLevelSupplier is null
	 */
	public TreeWorker(TreeWorkerCat<T> catFunction, TreeWorkerSupplier<S> baseLevelSupplier, TreeWorkerDelete<T> delete,
			TreeWorkerMap<S, T> mapFunction, IntFunction<T[]> arrayBuilder, int workers, int nodePerMerge)
			throws TreeWorkerException {
		this.catFunction = Objects.requireNonNull(catFunction, "catFunction can't be null!");
		this.mapFunction = Objects.requireNonNull(mapFunction, "mapFunction can't be null!");
		this.baseLevelSupplier = Objects.requireNonNull(baseLevelSupplier, "baseLevelSupplier can't be null!");
		this.arrayBuilder = Objects.requireNonNull(arrayBuilder, "arrayBuilder can't be null!");
		if (delete == null) {
			this.delete = (t) -> {};
		} else {
			this.delete = delete;
		}
		if (nodePerMerge <= 0) {
			throw new TreeWorkerException("nodePerMerge count can't be <= 0!");
		}
		treeCount = 1 << nodePerMerge;
		if (workers <= 0) {
			throw new TreeWorkerException("worker count can't be <= 0!");
		}
		S s = baseLevelSupplier.get();
		if (s == null) {
			throw new TreeWorkerException("no base element!");
		}
		suppliedElements.add(s);
		this.workers = new ArrayList<>(workers);
		for (int i = 0; i < workers; i++) {
			this.workers.add(new Worker());
		}
		workerWorking = workers;
	}

	/**
	 * create a generic array T[] of a size
	 *
	 * @param size the size
	 * @return the array
	 */
	private T[] createArray(int size) {
		T[] array = arrayBuilder.apply(size);
		assert array != null && array.length >= size
				: "array function should create an array with a size of a least size";
		return array;
	}

	/**
	 * set a listener for each worker
	 *
	 * @param listener the listener
	 */
	public void setListener(MultiThreadListener listener) {
		this.listener = listener;
	}

	/**
	 * Start the workers
	 */
	public void start() {
		synchronized (elements) {
			if (started) {
				throw new IllegalArgumentException("TreeWorker already started!");
			}
			for (Worker worker : this.workers) {
				worker.start();
			}
			started = true;
		}
	}

	/**
	 * delete all the elements
	 */
	private void clearData() {
		for (Element e : elements) {
			delete.delete(e.mappedValue);
		}
	}

	/**
	 * wait for the tree worker to complete
	 *
	 * @return the last element
	 * @throws TreeWorkerException  if an error occurred in a worker
	 * @throws InterruptedException in case of interruption
	 */
	public T waitToComplete() throws TreeWorkerException, InterruptedException {
		try {
			if (listener != null) {
				synchronized (WORKING_SYNC) {
					while (workerWorking > 0) {
						listener.notifyProgress(100F * (workers.size() - workerWorking) / workers.size(),
								"waiting for workers to complete " + (workers.size() - workerWorking) + "/"
										+ workers.size());
						WORKING_SYNC.wait();
					}
				}
			}
			for (Worker w : workers) {
				w.join();
			}

			if (listener != null) {
				listener.notifyProgress(100, "tree completed");
			}
		} catch (InterruptedException e) {
			clearData();
			throw e;
		}

		if (throwable != null) {
			clearData();
			throw throwable;
		}

		if (!fetchDone || !mapDone) {
			clearData();
			// shouldn't be possible?
			throw new TreeWorkerException("The worker isn't done!");
		}
		if (elements.isEmpty()) {
			return null;
		}
		return elements.get(0).mappedValue;
	}

	private int countBase() {
		return suppliedElements.size();
	}

	/**
	 * map function to map an element to another
	 *
	 * @param <T> old type
	 * @param <E> new type
	 * @author Antoine Willerval
	 */
	public interface TreeWorkerMap<T, E> {
		/**
		 * create an identity map function
		 *
		 * @param <T> the type
		 * @return map function
		 */
		static <T> TreeWorkerMap<T, T> identity() {
			return t -> t;
		}

		/**
		 * map the value
		 *
		 * @param prev the previous value
		 * @return the new value
		 */
		E map(T prev);
	}

	/**
	 * cat function to merge two elements
	 *
	 * @param <T> the elements type
	 * @author Antoine Willerval
	 */
	@FunctionalInterface
	public interface TreeWorkerCat<T> {
		/**
		 * construct an element from elements
		 *
		 * @param element the array of elements.
		 * @param count   the number of elements in the array, from index 0
		 *                (inclusive) to count (exclusive)
		 * @return the cat of the 2 elements
		 */
		T construct(T[] element, int count);
	}

	/**
	 * delete function in case of error
	 *
	 * @param <T> the elements type
	 * @author Antoine Willerval
	 */
	@FunctionalInterface
	public interface TreeWorkerDelete<T> {
		/**
		 * delete an unused element
		 *
		 * @param e the element to delete
		 */
		void delete(T e);
	}

	/**
	 * supply function
	 *
	 * @param <S> the elements type
	 * @author Antoine Willerval
	 */
	@FunctionalInterface
	public interface TreeWorkerSupplier<S> {
		/**
		 * supply an element to merge
		 *
		 * @return the element to merge
		 */
		S get();
	}

	/**
	 * Interface containing all the TreeWorker function to implement
	 *
	 * @param <S> Supplying type
	 * @param <T> Mapped type
	 * @author Antoine Willerval
	 */
	public interface TreeWorkerObject<S, T>
			extends TreeWorkerCat<T>, TreeWorkerSupplier<S>, TreeWorkerDelete<T>, TreeWorkerMap<S, T> {}

	/**
	 * Interface containing all the TreeWorker function to implement without the
	 * map operation
	 *
	 * @param <T> type
	 * @author Antoine Willerval
	 */
	public interface TreeWorkerObjectNoMap<T> extends TreeWorkerObject<T, T> {
		@Override
		default T map(T prev) {
			return prev;
		}
	}

	/**
	 * @return if the worker is completed
	 */
	public boolean isCompleted() {
		synchronized (elements) {
			return (fetchDone && mapDone && elements.size() <= 1) || throwable != null;
		}
	}

	private class Element {
		T mappedValue;
		int level;

		public Element(T mappedValue, int level) {
			this.mappedValue = mappedValue;
			this.level = level;
		}
	}

	private class Tuple {
		Element first;
		T[] elements;
		int count;
		int level;

		Tuple() {
			elements = createArray(treeCount);
			clear();
		}

		/**
		 * add an element to this tuple
		 *
		 * @param e the element
		 */
		public void addElement(Element e) {
			if (count == 0) {
				first = e;
				level = e.level;
			}
			elements[count++] = e.mappedValue;
			assert level == e.level : "add from different level";
		}

		/**
		 * @return the first element added since the last tuple reset/creation
		 */
		public Element getFirstElement() {
			return first;
		}

		/**
		 * remove all the elements from the tree worker elements
		 *
		 * @throws TreeWorkerException if an element can't be removed
		 */
		public void remove() throws TreeWorkerException {
			for (int i = 0; i < count; i++) {
				removeFirst(elements[i]);
			}
		}

		private void removeFirst(T element) throws TreeWorkerException {
			Iterator<Element> it = TreeWorker.this.elements.iterator();
			while (it.hasNext()) {
				Element e = it.next();
				if (e.mappedValue == element && e.level == level) {
					it.remove();
					return;
				}
			}
			throw new TreeWorkerException("Can't remove an elements! " + element);
		}

		/**
		 * @return the internal array inside, at least the size returned by
		 *         {@link #size()}
		 */
		public T[] getArray() {
			return elements;
		}

		/**
		 * @return the count of elements
		 */
		public int size() {
			return count;
		}

		/**
		 * reset the tuple
		 */
		public void clear() {
			this.count = 0;
		}

		/**
		 * get a element in a particular index
		 *
		 * @param index the index
		 * @return the element
		 */
		public T get(int index) {
			return elements[index];
		}

		private int searchDir(int start, int direction, int min) {
			if (direction < 0) {
				for (int i = start; i >= 0; i--) {
					searchAtLevel(i);
					if (size() >= min) {
						return i;
					}
				}
			} else {
				for (int i = start; i <= maxLevel; i++) {
					searchAtLevel(i);
					if (size() >= min) {
						return i;
					}
				}
			}
			return -1;
		}

		private void searchAtLevel(int level) {
			clear();
			synchronized (TreeWorker.this.elements) {
				for (Element e : TreeWorker.this.elements) {
					if (e.level == level) {
						addElement(e);
						if (count == treeCount) {
							return;
						}
					}
				}
			}
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
			synchronized (FETCH_SYNC) {
				if (fetchDone) {
					return; // another fetch job won
				}
				S s = baseLevelSupplier.get();
				synchronized (elements) {
					if (s == null) {
						fetchDone = true;
						// say if all the mapping is done, only after the fetch
						// was done
						if (suppliedElements.isEmpty()) {
							mapDone = true;
						}
					} else {
						suppliedElements.add(s);
					}
					elements.notifyAll();
				}
			}
		}
	}

	private class Map extends TreeWorkerJob {
		S old;

		public Map(S old) {
			this.old = old;
		}

		@Override
		public void runJob() {
			// map the supplied value
			T mappedValue = mapFunction.map(old);

			synchronized (TreeWorker.this.elements) {
				// add it to the element list
				TreeWorker.this.elements.add(new Element(mappedValue, 0));

				// say if all the mapping is done, only after the fetch was done
				if (fetchDone && suppliedElements.isEmpty()) {
					mapDone = true;
				}
				elements.notifyAll();
			}
		}
	}

	private class Merge extends TreeWorkerJob {
		T[] elements;
		int count;
		int level;

		public Merge(T[] elements, int count, int level) {
			this.elements = elements;
			this.count = count;
			this.level = level;
			assert count > 0 : "cat from empty element!";
		}

		@Override
		public void runJob() {
			T t = catFunction.construct(elements, count);
			synchronized (TreeWorker.this.elements) {
				TreeWorker.this.elements.add(new Element(t, level + 1));
				maxLevel = Math.max(maxLevel, level + 1);
			}
		}

		@Override
		void clear() {
			for (int i = 0; i < count; i++) {
				delete.delete(elements[i]);
			}
		}
	}

	private class Worker extends Thread {
		// array used to get merge object
		private final Tuple tuple = new Tuple();

		public Worker() {
			super("JobWorker#" + JOB_ID_NAME.incrementAndGet());
		}

		@Override
		public void run() {
			try {
				while (!isCompleted()) {
					if (listener != null) {
						listener.notifyProgress(0, "waiting job");
					}
					TreeWorkerJob job = null;
					try {
						synchronized (WAITING_SYNC) {
							job = getJob();
							if (job == null) {
								if (isCompleted()) {
									return;
								}
								workerWaiting++;
								WAITING_SYNC.wait();
								--workerWaiting;
								continue;
							}
						}
						job.runJob();
						synchronized (WAITING_SYNC) {
							if (workerWaiting > 0) {
								WAITING_SYNC.notify();
							}
						}
					} catch (Throwable t) {
						if (job != null) {
							job.clear();
						}
						synchronized (elements) {
							if (throwable != null) {
								throwable.addSuppressed(t);
							}
							if (t instanceof TreeWorkerException) {
								throwable = (TreeWorkerException) t;
							} else {
								throwable = new TreeWorkerException(t);
							}
							elements.notifyAll();
						}
						synchronized (WAITING_SYNC) {
							WAITING_SYNC.notifyAll();
						}
					}
				}
			} finally {
				if (listener != null) {
					listener.notifyProgress(100, "completed");
					listener.unregisterThread(getName());
				}
				synchronized (WORKING_SYNC) {
					workerWorking--;
					WORKING_SYNC.notify();
				}
			}
		}

		private TreeWorkerJob getJob() throws TreeWorkerException {
			synchronized (elements) {
				while (true) {
					if (mapDone) {
						if (elements.size() == 1) {
							return null; // end, no ascend/merge required
						}
						int level = tuple.searchDir(0, 1, 1);
						if (level == -1) {
							return null; // size == 0 end
						}
						if (tuple.size() == 1) {
							tuple.getFirstElement().level++;
						} else { // size == 2
							tuple.remove();
							return new Merge(tuple.getArray(), tuple.size(), level);
						}
					} else {
						if (fetchDone) {
							if (suppliedElements.isEmpty()) {
								// edge case if we are waiting for a map to
								// complete, Fetch won't do anything
								return new Fetch();
							}
							return new Map(suppliedElements.remove(0));
						}
						// count the number of supplied elements to know if we
						// need to fetch another one
						int level0 = countBase();
						if (workers.size() != 1 && level0 < workers.size() / 2) {
							return new Fetch();
						}
						// search for a merge candidate with the size treeCount
						int level = tuple.searchDir(maxLevel, -1, treeCount);

						if (level != -1) {
							// remove the component of the candidate and merge
							// them
							tuple.remove();
							return new Merge(tuple.getArray(), tuple.size(), level);
						}

						if (suppliedElements.isEmpty()) {
							// no supplied element to map, we fetch a new one
							return new Fetch();
						} else {
							// map the supplied element
							return new Map(suppliedElements.remove(0));
						}
					}
				}
			}
		}
	}

	/**
	 * An exception in the tree worker
	 *
	 * @author Antoine Willerval
	 */
	public static class TreeWorkerException extends Exception {
		public TreeWorkerException(Throwable cause) {
			super(cause);
		}

		public TreeWorkerException(String message) {
			super(message);
		}
	}
}
