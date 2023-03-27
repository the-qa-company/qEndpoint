package com.the_qa_company.qendpoint.utils.sail;

import com.the_qa_company.qendpoint.utils.sail.filter.SailFilter;
import com.the_qa_company.qendpoint.utils.sail.linked.LinkedSail;
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
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Filtering sail to bypass a or multiple sails
 *
 * @author Antoine Willerval
 */
public class FilteringSail implements NotifyingSail, LinkedSail<FilteringSail> {
	private File dataDir;
	private final NotifyingSail onYesSail;
	private final Consumer<Sail> endSail;
	private final BiFunction<FilteringSail, SailConnection, SailFilter> filter;
	private MultiInputFilteringSail onNoSail;

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail  the sail if the filter returns true
	 * @param sourceSail the sail if the filter returns false
	 * @param filter     the filter building method (if connection required)
	 */
	public FilteringSail(LinkedSail<? extends NotifyingSail> onYesSail, NotifyingSail sourceSail,
			BiFunction<FilteringSail, SailConnection, SailFilter> filter) {
		this(onYesSail.getSail(), sourceSail, onYesSail.getSailConsumer(), filter);
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail  the sail if the filter returns true
	 * @param sourceSail the sail if the filter returns false
	 * @param filter     the filter building method
	 */
	public FilteringSail(LinkedSail<? extends NotifyingSail> onYesSail, NotifyingSail sourceSail, SailFilter filter) {
		this(onYesSail.getSail(), sourceSail, onYesSail.getSailConsumer(), filter);
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail  the sail if the filter returns true
	 * @param sourceSail the sail if the filter returns false
	 * @param endSail    a consumer to set the end sail of the onYesSail
	 * @param filter     the filter building method (if connection required)
	 */
	public FilteringSail(NotifyingSail onYesSail, NotifyingSail sourceSail, Consumer<Sail> endSail,
			BiFunction<FilteringSail, SailConnection, SailFilter> filter) {
		this.onYesSail = Objects.requireNonNull(onYesSail, "onYesSail can't be null!");
		this.endSail = endSail;
		this.filter = Objects.requireNonNull(filter, "filter can't be null!");
		if (sourceSail != null) {
			setSourceSail(sourceSail);
		}
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail  the sail if the filter returns true
	 * @param sourceSail the sail if the filter returns false
	 * @param endSail    a consumer to set the end sail of the onYesSail
	 * @param filter     the filter
	 */
	public FilteringSail(NotifyingSail onYesSail, NotifyingSail sourceSail, Consumer<Sail> endSail, SailFilter filter) {
		this(onYesSail, sourceSail, endSail, (sail, conn) -> filter);
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail the sail if the filter returns true
	 * @param filter    the filter building method (if connection required)
	 */
	public FilteringSail(LinkedSail<? extends NotifyingSail> onYesSail,
			BiFunction<FilteringSail, SailConnection, SailFilter> filter) {
		this(onYesSail.getSail(), null, onYesSail.getSailConsumer(), filter);
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail the sail if the filter returns true
	 * @param filter    the filter building method
	 */
	public FilteringSail(LinkedSail<? extends NotifyingSail> onYesSail, SailFilter filter) {
		this(onYesSail.getSail(), null, onYesSail.getSailConsumer(), filter);
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail the sail if the filter returns true
	 * @param endSail   a consumer to set the end sail of the onYesSail
	 * @param filter    the filter building method (if connection required)
	 */
	public FilteringSail(NotifyingSail onYesSail, Consumer<Sail> endSail,
			BiFunction<FilteringSail, SailConnection, SailFilter> filter) {
		this(onYesSail, null, endSail, filter);
	}

	/**
	 * create a filtering sail.
	 *
	 * @param onYesSail the sail if the filter returns true
	 * @param endSail   a consumer to set the end sail of the onYesSail
	 * @param filter    the filter
	 */
	public FilteringSail(NotifyingSail onYesSail, Consumer<Sail> endSail, SailFilter filter) {
		this(onYesSail, null, endSail, (sail, conn) -> filter);
	}

	public void setSourceSail(NotifyingSail sourceSail) {
		this.onNoSail = new MultiInputFilteringSail(sourceSail, this);
		endSail.accept(this.onNoSail);
	}

	public void setBaseSail(Sail sail) {
		setSourceSail((NotifyingSail) sail);
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
	public synchronized NotifyingSailConnection getConnection() throws SailException {
		onNoSail.startCreatingConnection();

		NotifyingSailConnection connection = new FilteringSailConnection(onYesSail.getConnection(),
				onNoSail.getConnection(), this);

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
	public MultiInputFilteringSail getOnNoSail() {
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
		return getFilter(onNoSail.getConnection());
	}

	/**
	 * create a filter, can be used for the same connection
	 *
	 * @param connection the connection to build the filter
	 * @return filter
	 */
	public SailFilter getFilter(SailConnection connection) throws SailException {
		return Objects.requireNonNull(filter.apply(this, connection), "Created filter is null!");
	}

	@Override
	public IsolationLevel getDefaultIsolationLevel() {
		return onYesSail.getDefaultIsolationLevel();
	}

	@Override
	public FilteringSail getSail() {
		return this;
	}

	@Override
	public Consumer<Sail> getSailConsumer() {
		return this::setBaseSail;
	}
}
