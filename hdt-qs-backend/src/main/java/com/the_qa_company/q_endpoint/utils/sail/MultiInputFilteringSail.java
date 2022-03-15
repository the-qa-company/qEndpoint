package com.the_qa_company.q_endpoint.utils.sail;

import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;

/**
 * A sail to create multiple Connection input points.
 *
 * To create a new connection, the user should call {@link #startCreatingConnection()}, {@link #getConnection()} and
 * complete it with {@link #completeCreatingConnection()}.
 *
 * <pre>
 * // prepare new connection
 * sail.startCreatingConnection();
 *
 * // get a the connection (can be used how much you want, but at least once to close it)
 * sail.getConnection();
 *
 * // complete the creation
 * sail.completeCreatingConnection();
 * </pre>
 *
 * @author Antoine Willerval
 */
class MultiInputFilteringSail extends NotifyingSailWrapper {
	private final LockManager lockManager = new LockManager();
	private Lock lock;
	private MultiInputFilteringSailConnection lastConnection;
	private final FilteringSail filteringSail;

	/**
	 * Creates a new MultiInputSail object that wraps the supplied connection.
	 *
	 * @param wrappedSail the sail to allow multiple sail
	 */
	public MultiInputFilteringSail(NotifyingSail wrappedSail, FilteringSail filteringSail) {
		super(wrappedSail);
		this.filteringSail = filteringSail;
	}

	/**
	 * start a new connection creation, the next call to {@link #getConnection()} will return the same connection.
	 * @throws SailException if the creating is interrupted
	 */
	public synchronized void startCreatingConnection() throws SailException {
		try {
			lockManager.waitForActiveLocks();
		} catch (InterruptedException e) {
			throw new SailException("Interruption while waiting for active locks", e);
		}

		lock = lockManager.createLock("");

		lastConnection = new MultiInputFilteringSailConnection(getBaseSail().getConnection());
	}

	@Override
	public synchronized MultiInputFilteringSailConnection getConnection() throws SailException {
		checkCreatingConnectionStarted();
		return lastConnection;
	}

	/**
	 * complete a new connection creation.
	 * @throws SailException if {@link #startCreatingConnection()} wasn't called before
	 */
	public synchronized void completeCreatingConnection() throws SailException {
		checkCreatingConnectionStarted();
		lastConnection.setFilter(filteringSail.getFilter());
		lock.release();
		lastConnection = null;
	}

	public void checkCreatingConnectionStarted() throws SailException {
		if (lastConnection == null) {
			throw new SailException("The MultiInputSail wasn't started!");
		}
	}

	public FilteringSail getFilteringSail() {
		return filteringSail;
	}
}
