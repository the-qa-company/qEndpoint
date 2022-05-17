package com.the_qa_company.qendpoint.utils.sail;

import com.the_qa_company.qendpoint.utils.sail.filter.SailFilter;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailConnectionListener;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailConnectionWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Antoine Willerval
 */
class MultiInputFilteringSailConnection extends NotifyingSailConnectionWrapper {
	private final List<SailConnectionListener> yesListeners = new ArrayList<>();
	private final List<SailConnectionListener> noListeners = new ArrayList<>();
	private SailFilter filter;

	public MultiInputFilteringSailConnection(NotifyingSailConnection wrappedCon) {
		super(wrappedCon);
	}

	void setFilter(SailFilter newFilter) {
		boolean init = this.filter != null;
		this.filter = Objects.requireNonNull(newFilter, "newFilter can't be null!");

		if (init) {
			return;
		}

		SailConnectionListener listener = new SailConnectionListener() {

			@Override
			public void statementAdded(Statement st) {
				if (filter.shouldHandleNotifyAdd(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext())) {
					synchronized (yesListeners) {
						for (SailConnectionListener listener : yesListeners) {
							listener.statementAdded(st);
						}
					}
				} else {
					synchronized (noListeners) {
						for (SailConnectionListener listener : noListeners) {
							listener.statementAdded(st);
						}
					}
				}
			}

			@Override
			public void statementRemoved(Statement st) {
				if (filter.shouldHandleNotifyRemove(st.getSubject(), st.getPredicate(), st.getObject(),
						st.getContext())) {
					synchronized (yesListeners) {
						for (SailConnectionListener listener : yesListeners) {
							listener.statementRemoved(st);
						}
					}
				} else {
					synchronized (noListeners) {
						for (SailConnectionListener listener : noListeners) {
							listener.statementRemoved(st);
						}
					}
				}
			}

		};
		super.addConnectionListener(listener);
	}

	@Override
	public void removeConnectionListener(SailConnectionListener listener) {
		synchronized (yesListeners) {
			yesListeners.remove(listener);
		}
	}

	@Override
	public void addConnectionListener(SailConnectionListener listener) {
		synchronized (yesListeners) {
			yesListeners.add(listener);
		}
	}

	/**
	 * add a listener to bypass the yes sail
	 *
	 * @param listener the listener
	 */
	public void removeBypassConnectionListener(SailConnectionListener listener) {
		synchronized (noListeners) {
			noListeners.remove(listener);
		}
	}

	/**
	 * remove a listener to bypass the yes sail
	 *
	 * @param listener the listener
	 */
	public void addBypassConnectionListener(SailConnectionListener listener) {
		synchronized (noListeners) {
			noListeners.add(listener);
		}
	}

}
