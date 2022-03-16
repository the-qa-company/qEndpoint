package com.the_qa_company.q_endpoint.utils.sail;

import com.the_qa_company.q_endpoint.utils.sail.filter.SailFilter;
import org.eclipse.rdf4j.common.transaction.IsolationLevel;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailChangedListener;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Filtering sail to bypass a or multiple sails
 *
 * @author Antoine Willerval
 */
public class FilteringSail implements NotifyingSail {
	private File dataDir;
	private final NotifyingSail onYesSail;
	private final MultiInputFilteringSail onNoSail;
	private final Function<SailConnection, SailFilter> filter;

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail the sail if the filter returns true
	 * @param onNoSail  the sail if the filter returns false
	 * @param endSail   a consumer to set the end sail of the onYesSail
	 * @param filter    the filter building method (if connection required)
	 */
	public FilteringSail(NotifyingSail onYesSail, NotifyingSail onNoSail, Consumer<Sail> endSail, Function<SailConnection, SailFilter> filter) {
		Objects.requireNonNull(onNoSail, "onNoSail can't be null!");
		this.onYesSail = Objects.requireNonNull(onYesSail, "onYesSail can't be null!");
		this.onNoSail = new MultiInputFilteringSail(onNoSail, this);
		endSail.accept(this.onNoSail);
		this.filter = Objects.requireNonNull(filter, "filter can't be null!");
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail the sail if the filter returns true
	 * @param onNoSail  the sail if the filter returns false
	 * @param endSail   a consumer to set the end sail of the onYesSail
	 * @param filter    the filter
	 */
	public FilteringSail(NotifyingSail onYesSail, NotifyingSail onNoSail, Consumer<Sail> endSail, SailFilter filter) {
		this(onYesSail, onNoSail, endSail, co -> filter);
	}

	@Override
	public void setDataDir(File file) {
		dataDir = file;
	}

	@Override
	public File getDataDir() {
		return dataDir;
	}

	@Override
	public void init() throws SailException {
		onYesSail.init();
	}

	@Override
	public void shutDown() throws SailException {
		onYesSail.shutDown();
	}

	@Override
	public boolean isWritable() throws SailException {
		return onYesSail.isWritable();
	}

	@Override
	public NotifyingSailConnection getConnection() throws SailException {
		onNoSail.startCreatingConnection();

		NotifyingSailConnection connection = new FilteringSailConnection(
				onYesSail.getConnection(),
				onNoSail.getConnection(),
				this
		);

		onNoSail.completeCreatingConnection();

		return connection;
	}

	@Override
	public void addSailChangedListener(SailChangedListener listener) {
		onYesSail.addSailChangedListener(listener);
	}

	@Override
	public void removeSailChangedListener(SailChangedListener listener) {
		onYesSail.removeSailChangedListener(listener);
	}

	@Override
	public ValueFactory getValueFactory() {
		return onYesSail.getValueFactory();
	}

	@Override
	public List<IsolationLevel> getSupportedIsolationLevels() {
		return onYesSail.getSupportedIsolationLevels();
	}

	/**
	 * @return the sail if the filter returns false
	 */
	public Sail getOnNoSail() {
		return onNoSail;
	}

	/**
	 * @return the sail if the filter returns true
	 */
	public Sail getOnYesSail() {
		return onYesSail;
	}

	/**
	 * @return create a filter, can be used for the same connection
	 */
	public SailFilter getFilter() throws SailException {
		onNoSail.checkCreatingConnectionStarted();
		return Objects.requireNonNull(filter.apply(onNoSail.getConnection()), "Created filter is null!");
	}

	@Override
	public IsolationLevel getDefaultIsolationLevel() {
		return onYesSail.getDefaultIsolationLevel();
	}

}
