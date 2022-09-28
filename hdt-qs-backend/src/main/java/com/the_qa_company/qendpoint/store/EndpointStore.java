package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.model.EndpointStoreValueFactory;
import com.the_qa_company.qendpoint.model.SimpleIRIHDT;
import com.the_qa_company.qendpoint.utils.BitArrayDisk;
import com.the_qa_company.qendpoint.utils.CloseSafeHDT;
import org.eclipse.rdf4j.common.concurrent.locks.Lock;
import org.eclipse.rdf4j.common.concurrent.locks.LockManager;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverClient;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.ntriples.NTriplesWriter;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.base.SailStore;
import org.eclipse.rdf4j.sail.helpers.AbstractNotifyingSail;
import org.eclipse.rdf4j.sail.helpers.DirectoryLockManager;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class EndpointStore extends AbstractNotifyingSail implements FederatedServiceResolverClient {
	private static final AtomicLong ENDPOINT_DEBUG_ID_GEN = new AtomicLong();
	private static final Logger logger = LoggerFactory.getLogger(EndpointStore.class);
	private final long debugId;
	// HDT file containing the data
	private CloseSafeHDT hdt;
	// specs of the HDT file
	private HDTSpecification spec;
	private HDTConverter hdtConverter;

	// some cached information about the HDT store
	private HDTProps hdtProps;

	// stores to store the delta
	public AbstractNotifyingSail nativeStoreA;
	public AbstractNotifyingSail nativeStoreB;

	// location of the native store

	// bitmap to mark which triples in HDT were deleted
	private BitArrayDisk deleteBitMap;
	// bitmap used to mark deleted triples in HDT during a merge operation
	// FIXME: is this needed?
	private BitArrayDisk tempdeleteBitMap;
	// setting to put the delete map only in memory, i.e don't write to disk
	private final boolean inMemDeletes;
	private final boolean loadIntoMemory;

	// bitmaps used to mark if the subject, predicate, object elements in HDT
	// are used in the rdf4j delta store
	private BitArrayDisk bitX;
	private BitArrayDisk bitY;
	private BitArrayDisk bitZ;

	// marks if store A or store B is used
	public boolean switchStore = false;
	// file storing which store is used
	File checkFile;

	// flag if the store is merging or not
	private boolean isMerging = false;

	public boolean isMergeTriggered = false;

	private boolean freezeNotifications = false;

	// threshold above which the merge process is starting
	private int threshold;

	EndpointStoreValueFactory valueFactory;

	private NTriplesWriter rdfWriterTempTriples;

	// lock manager for the merge thread
	public final LockManager lockToPreventNewConnections;
	// lock manager for the connections over the current repository
	public final LockManager locksHoldByConnections;
	// lock manager for the updates in the merge thread
	public final LockManager lockToPreventNewUpdate;
	public final LockManager locksHoldByUpdates;

	// variable counting the current number of triples in the delta
	public long triplesCount;

	private final MergeRunnable mergeRunnable;
	private final EndpointFiles endpointFiles;
	private MergeRunnable.MergeThread<?> mergerThread;

	private void deleteNativeLocks() {
		// remove lock files of a hard shutdown (SAIL is already locked by
		// [...])
		new DirectoryLockManager(this.nativeStoreA.getDataDir()).revokeLock();
		new DirectoryLockManager(this.nativeStoreB.getDataDir()).revokeLock();
	}

	public EndpointStore(EndpointFiles files, HDTSpecification spec, boolean inMemDeletes, boolean loadIntoMemory)
			throws IOException {
		debugId = ENDPOINT_DEBUG_ID_GEN.incrementAndGet();
		EndpointStoreUtils.openEndpoint(this);
		this.endpointFiles = files;
		this.loadIntoMemory = loadIntoMemory;
		this.mergeRunnable = new MergeRunnable(this);
		logger.info("CHECK IF A PREVIOUS MERGE WAS STOPPED");
		Optional<MergeRunnable.MergeThread<?>> mergeThread = mergeRunnable.createRestartThread();
		mergeThread.ifPresent(MergeRunnable.MergeThread::preLoad);
		HDT hdt;

		if (loadIntoMemory) {
			hdt = HDTManager.loadIndexedHDT(endpointFiles.getHDTIndex(), null, spec);
		} else {
			hdt = HDTManager.mapIndexedHDT(endpointFiles.getHDTIndex(), spec, null);
		}

		this.nativeStoreA = new NativeStore(new File(getEndpointFiles().getNativeStoreA()), "spoc,posc,cosp");
		this.nativeStoreB = new NativeStore(new File(getEndpointFiles().getNativeStoreB()), "spoc,posc,cosp");

		Files.createDirectories(Path.of(getEndpointFiles().getLocationNative()));
		this.checkFile = new File(getEndpointFiles().getWhichStore());
		// init the store before creating the check store file
		if (MergeRunnableStopPoint.debug) {
			deleteNativeLocks();
		}
		this.nativeStoreA.init();
		this.nativeStoreB.init();
		checkWhichStore();
		resetHDT(hdt, false);
		this.valueFactory = new EndpointStoreValueFactory(hdt);
		this.threshold = 100000;

		this.inMemDeletes = inMemDeletes;
		this.hdtProps = new HDTProps(this.hdt);

		this.lockToPreventNewConnections = new LockManager();
		this.lockToPreventNewUpdate = new LockManager();
		this.locksHoldByConnections = new LockManager();
		this.locksHoldByUpdates = new LockManager();

		initDeleteArray();
		// load HDT file
		this.spec = spec;

		// initialize the count of the triples
		mergeThread.ifPresent(thread -> {
			isMergeTriggered = true;
			mergerThread = thread;
			thread.start();
			logger.info("MERGE RESTART THREAD LAUNCHED");
		});
		SailConnection connection = getChangingStore().getConnection();
		this.triplesCount = connection.size();
		connection.close();
	}

	public EndpointStore(String locationHdt, String hdtIndexName, HDTSpecification spec, String locationNative,
			boolean inMemDeletes, boolean loadIntoMemory) throws IOException {
		this(new EndpointFiles(locationNative, locationHdt, hdtIndexName), spec, inMemDeletes, loadIntoMemory);
	}

	public EndpointStore(String locationHdt, String hdtIndexName, HDTSpecification spec, String locationNative,
			boolean inMemDeletes) throws IOException {
		this(locationHdt, hdtIndexName, spec, locationNative, inMemDeletes, false);
	}

	public void initNativeStoreDictionary(HDT hdt) throws IOException {
		this.bitX = new BitArrayDisk(hdt.getDictionary().getNsubjects(), endpointFiles.getHDTBitX());
		this.bitY = new BitArrayDisk(hdt.getDictionary().getNpredicates(), endpointFiles.getHDTBitY());
		this.bitZ = new BitArrayDisk(hdt.getDictionary().getNobjects() - hdt.getDictionary().getNshared(),
				endpointFiles.getHDTBitZ());
		// if the bitmaps have not been initialized with the native store
		if (this.bitX.countOnes() == 0 && this.bitY.countOnes() == 0 && this.bitZ.countOnes() == 0) {
			initBitmaps();
		}
	}

	public void resetHDT(HDT hdt, boolean closeOld) throws IOException {
		if (closeOld && this.hdt != null) {
			try {
				this.hdt.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		this.setHdt(hdt);
		this.setHdtProps(new HDTProps(hdt));
		initNativeStoreDictionary(this.hdt);
		this.setValueFactory(new EndpointStoreValueFactory(hdt));
		this.hdtConverter = new HDTConverter(this);
	}

	/**
	 * set the threshold before a merge is automatically made.
	 *
	 * @param threshold the threshold, 0 for disabling the automatic merge
	 */
	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

	// init the delete array upon the first start of the store
	private void initDeleteArray() throws IOException {
		if (this.inMemDeletes)
			setDeleteBitMap(new BitArrayDisk(this.hdt.getTriples().getNumberOfElements()));
		else {
			// @todo: these should be recovered from the file if it is there
			setDeleteBitMap(
					new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(), endpointFiles.getTripleDeleteArr()));
		}
	}

	@Override
	protected void initializeInternal() throws SailException {
		// this.repo.init();
		this.nativeStoreA.init();
		this.nativeStoreB.init();
	}

	public void checkWhichStore() throws IOException {
		if (!checkFile.exists() || !checkFile.isFile()) {
			// file does not exist, so this is the first time running the
			// program.
			Files.writeString(checkFile.toPath(), Boolean.toString(switchStore));
		} else {
			// This file exists, we already ran the program previously, just
			// read the value
			this.switchStore = Boolean.parseBoolean(Files.readString(checkFile.toPath()));
		}
	}

	public void writeWhichStore() throws IOException {
		Files.writeString(checkFile.toPath(), Boolean.toString(switchStore));
	}

	/**
	 * @return the threshold before a store merge, a non-positive value means
	 *         that the automatic merge is disabled
	 */
	public int getThreshold() {
		return threshold;
	}

	public Sail getChangingStore() {
		if (switchStore) {
			logger.debug("Changing store is B");
			return nativeStoreB;
		} else {
			logger.debug("Changing store is A");
			return nativeStoreA;
		}

	}

	public Sail getFreezedStoreStore() {
		if (!switchStore) {
			logger.debug("Freezed store is B");
			return nativeStoreB;
		} else {
			logger.debug("Freezed store is A");
			return nativeStoreA;
		}

	}

	// force access to the store via reflection, the library does not allow
	// directly since the method is protected
	public SailStore getCurrentSaliStore() {
		try {
			Sail sail = getChangingStore();
			Method method = sail.getClass().getDeclaredMethod("getSailStore");
			method.setAccessible(true);
			return (SailStore) method.invoke(sail);
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new Error("Can't getCurrentSaliStore", e);
		}
	}

	public HDT getHdt() {
		return hdt;
	}

	public void setHdt(HDT hdt) {
		if (hdt instanceof CloseSafeHDT) {
			this.hdt = (CloseSafeHDT) hdt;
		} else {
			this.hdt = new CloseSafeHDT(hdt);
		}
	}

	@Override
	protected void shutDownInternal() throws SailException {
		// check also that the merge thread is finished
		logger.info("Shutdown merge");
		try {
			try {
				if (mergerThread != null) {
					mergerThread.join();
				}
			} finally {
				try {
					hdt.close();
				} finally {
					try {
						logger.info("Shutdown A");
						this.nativeStoreA.shutDown();
					} finally {
						logger.info("Shutdown B");
						this.nativeStoreB.shutDown();
					}
				}
			}
		} catch (InterruptedException | IOException e) {
			throw new SailException(e);
		}
		logger.info("Shutdown done");
	}

	@Override
	public void shutDown() throws SailException {
		EndpointStoreUtils.closeEndpoint(this);
		super.shutDown();
	}

	public boolean isLoadIntoMemory() {
		return loadIntoMemory;
	}

	@Override
	public boolean isWritable() throws SailException {
		if (switchStore) {
			return nativeStoreB.isWritable();
		} else {
			return nativeStoreA.isWritable();
		}
	}

	@Override
	public EndpointStoreValueFactory getValueFactory() {
		return this.valueFactory;
	}

	public void setValueFactory(EndpointStoreValueFactory valueFactory) {
		this.valueFactory = valueFactory;
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal() throws SailException {
		try {
			return new EndpointStoreConnection(this);
		} catch (Exception var2) {
			throw new SailException(var2);
		}
	}

	@Override
	protected void connectionClosed(SailConnection connection) {
		super.connectionClosed(connection);
		connection.close();
	}

	@Override
	public void setFederatedServiceResolver(FederatedServiceResolver federatedServiceResolver) {
		// nativeStoreA.setFederatedServiceResolver(federatedServiceResolver);
		// nativeStoreB.setFederatedServiceResolver(federatedServiceResolver);
	}

	public RepositoryConnection getConnectionToChangingStore() {
		return new SailRepository(getChangingStore()).getConnection();
	}

	public RepositoryConnection getConnectionToFreezedStore() {
		return new SailRepository(getFreezedStoreStore()).getConnection();
	}

	public boolean isMerging() {
		return isMerging;
	}

	public void setMerging(boolean merging) {
		isMerging = merging;
	}

	public NotifyingSail getNativeStoreA() {
		return nativeStoreA;
	}

	public NotifyingSail getNativeStoreB() {
		return nativeStoreB;
	}

	public HDTProps getHdtProps() {
		return hdtProps;
	}

	public void setHdtProps(HDTProps hdtProps) {
		this.hdtProps = hdtProps;
	}

	public BitArrayDisk getDeleteBitMap() {
		return deleteBitMap;
	}

	public void setDeleteBitMap(BitArrayDisk deleteBitMap) {
		this.deleteBitMap = deleteBitMap;
	}

	public BitArrayDisk getTempDeleteBitMap() {
		return tempdeleteBitMap;
	}

	public NTriplesWriter getRdfWriterTempTriples() {
		return rdfWriterTempTriples;
	}

	/*
	 * In case of merge, we create a new array to recover all deleted triples
	 * while merging
	 */
	public void initTempDeleteArray() throws IOException {
		this.tempdeleteBitMap = new BitArrayDisk(this.hdt.getTriples().getNumberOfElements(),
				endpointFiles.getTripleDeleteTempArr());
		this.tempdeleteBitMap.force(false);
	}

	/**
	 * Init temp file to store triples to be deleted from native store while
	 * merging
	 *
	 * @param isRestarting if we should append to previous data
	 */
	public void initTempDump(boolean isRestarting) {
		try {
			File file = new File(endpointFiles.getTempTriples());
			if (!file.exists())
				Files.createFile(file.toPath());
			FileOutputStream out = new FileOutputStream(file, isRestarting);
			this.rdfWriterTempTriples = new NTriplesWriter(out);
			this.rdfWriterTempTriples.startRDF();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// creates a new array that marks the deleted triples in the new HDT file
	public void resetDeleteArray(HDT newHdt) throws IOException {
		// delete array created at merge time

		BitArrayDisk newDeleteArray = new BitArrayDisk(newHdt.getTriples().getNumberOfElements());

		long lastOldSubject = -2;
		long lastNewSubject = -2;

		long lastOldPredicate = -2;
		long lastNewPredicate = -2;

		long lastOldObject = -2;
		long lastNewObject = -2;

		// @todo: remove debug count
		int debugSavedSubject = 0;
		int debugSavedPredicate = 0;
		int debugSavedObject = 0;
		int debugTotal = 0;
		StopWatch watch = new StopWatch();

		// iterate over the temp array, convert the triples and mark it as
		// deleted in the new HDT file
		for (long i = 0; i < tempdeleteBitMap.getNumBits(); i++) {
			if (tempdeleteBitMap.access(i)) { // means that a triple has been
												// deleted during merge
				// find the deleted triple in the old HDT index
				TripleID tripleID = this.hdt.getTriples().findTriple(i);
				if (tripleID.isValid()) {
					long oldSubject = tripleID.getSubject();
					long oldPredicate = tripleID.getPredicate();
					long oldObject = tripleID.getObject();

					// check if the last subject was this subject

					long subject;

					if (oldSubject != lastOldSubject) {
						subject = newHdt.getDictionary().stringToId(
								this.hdt.getDictionary().idToString(oldSubject, TripleComponentRole.SUBJECT),
								TripleComponentRole.SUBJECT);
						lastNewSubject = subject;
						lastOldSubject = oldSubject;
					} else {
						subject = lastNewSubject;
						debugSavedSubject++;
					}

					// check if the last predicate was this predicate

					long predicate;

					if (oldPredicate != lastOldPredicate) {
						predicate = newHdt.getDictionary().stringToId(
								this.hdt.getDictionary().idToString(oldPredicate, TripleComponentRole.PREDICATE),
								TripleComponentRole.PREDICATE);
						lastNewPredicate = predicate;
						lastOldPredicate = oldPredicate;
					} else {
						predicate = lastNewPredicate;
						debugSavedPredicate++;
					}

					// check if the last object was this object

					long object;

					if (oldObject != lastOldObject) {
						object = newHdt.getDictionary().stringToId(
								this.hdt.getDictionary().idToString(oldObject, TripleComponentRole.OBJECT),
								TripleComponentRole.OBJECT);
						lastNewObject = object;
						lastOldObject = oldObject;
					} else {
						object = lastNewObject;
						debugSavedObject++;
					}
					debugTotal++;

					// search over the given triple with the ID so that we can
					// mark the new array..
					TripleID triple = new TripleID(subject, predicate, object);

					if (!triple.isNoMatch()) {
						IteratorTripleID next = newHdt.getTriples().search(triple);
						if (next.hasNext()) {
							next.next();
							long newIndex = next.getLastTriplePosition();
							newDeleteArray.set(newIndex, true);
						}
					}
				}
			}
		}

		if (MergeRunnableStopPoint.debug) {
			logger.debug("HDT cache saved element(s) ones={} in {}", tempdeleteBitMap.countOnes(), watch.stopAndShow());
			if (debugTotal != 0) {
				logger.debug("debugSavedSubject        : {} % | {} / {}", 100 * debugSavedSubject / debugTotal,
						debugSavedSubject, debugTotal);
				logger.debug("debugSavedPredicate      : {} % | {} / {}", 100 * debugSavedPredicate / debugTotal,
						debugSavedPredicate, debugTotal);
				logger.debug("debugSavedObject         : {} % | {} / {}", 100 * debugSavedObject / debugTotal,
						debugSavedObject, debugTotal);
			} else {
				logger.debug("no remap");
			}
			logger.debug("Tmp map: {}", tempdeleteBitMap.printInfo());
			logger.debug("New map: {}", newDeleteArray.printInfo());
		}

		getDeleteBitMap().close();
		newDeleteArray.changeToInDisk(new File(endpointFiles.getTripleDeleteArr()));
		this.setDeleteBitMap(newDeleteArray);
	}

	public void markDeletedTempTriples() throws IOException {
		this.rdfWriterTempTriples.endRDF();
		try (InputStream inputStream = new FileInputStream(endpointFiles.getTempTriples())) {
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
			rdfParser.getParserConfig().set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
			try (GraphQueryResult res = QueryResults.parseGraphBackground(inputStream, null, rdfParser,
					null)) {
				while (res.hasNext()) {
					Statement st = res.next();
					IteratorTripleString search = this.hdt.search(st.getSubject().toString(),
							st.getPredicate().toString(), st.getObject().toString());
					if (search.hasNext()) {
						search.next();
						long index = search.getLastTriplePosition();
						if (index >= 0) {
							this.deleteBitMap.set(index, true);
						}
					}
				}
			} catch (NotFoundException e) {
				// shouldn't happen
				throw new RuntimeException(e);
			}
		}
	}

	// called from a locked block
	private void initBitmaps() {
		logger.debug("Resetting bitmaps");
		HDTConverter converter = new HDTConverter(this);
		// iterate over the current rdf4j store and mark in HDT the store
		// the subject, predicate, objects that are
		// used in rdf4j
		try (RepositoryConnection connection = this.getConnectionToChangingStore();
				RepositoryResult<Statement> statements = connection.getStatements(null, null, null)) {
			for (Statement statement : statements) {
				Resource internalSubj = converter.rdf4jToHdtIDsubject(statement.getSubject());
				IRI internalPredicate = converter.rdf4jToHdtIDpredicate(statement.getPredicate());
				Value internalObject = converter.rdf4jToHdtIDobject(statement.getObject());
				this.modifyBitmaps(internalSubj, internalPredicate, internalObject);
			}
		}
	}

	public void modifyBitmaps(Resource subject, IRI predicate, Value object) {
		// mark in HDT the store the subject, predicate, objects that are used
		// in rdf4j
		long subjectID = -1;
		if (subject instanceof SimpleIRIHDT) {
			subjectID = ((SimpleIRIHDT) subject).getId();
		}
		long predicateID = -1;
		if (predicate instanceof SimpleIRIHDT) {
			predicateID = ((SimpleIRIHDT) predicate).getId();
		}
		long objectID = -1;
		if (object instanceof SimpleIRIHDT) {
			objectID = ((SimpleIRIHDT) object).getId();
		}
		modifyBitmaps(subjectID, predicateID, objectID);
	}

	public boolean shouldSearchOverRDF4J(long subject, long predicate, long object) {
		if (subject != -1 && subject != 0 && !this.getBitX().access(subject - 1)) {
			return false;
		}

		if (predicate != -1 && predicate != 0 && !this.getBitY().access(predicate - 1))
			return false;

		if (object != -1 && object != 0) {
			if (object <= this.hdt.getDictionary().getNshared()) {
				return this.getBitX().access(object - 1);
			} else {
				return this.getBitZ().access(object - hdt.getDictionary().getNshared() - 1);
			}
		}
		return true;
	}

	public void modifyBitmaps(long subject, long predicate, long object) {
		// mark in HDT the store the subject, predicate, objects that are used
		// in rdf4j
		if (subject != -1 && subject != 0) {
			this.getBitX().set(subject - 1, true);
		}
		if (predicate != -1 && predicate != 0) {
			this.getBitY().set(predicate - 1, true);
		}
		if (object != -1 && object != 0) {
			if (object <= this.hdt.getDictionary().getNshared()) {
				this.getBitX().set(object - 1, true);
			} else {
				this.getBitZ().set(object - hdt.getDictionary().getNshared() - 1, true);
			}
		}
	}

	/**
	 * Ask for a merge of the store.
	 */
	public void mergeStore() throws MergeStartException {
		mergeStore(true);
	}

	private void failOrWarn(boolean fail, String msg) throws MergeStartException {
		if (fail) {
			throw new MergeStartException(msg);
		} else {
			logger.warn("{}", msg);
		}
	}

	private synchronized void mergeStore(boolean fail) throws MergeStartException {
		// check that no merge is already triggered
		if (isMergeTriggered) {
			failOrWarn(fail, "A merge was triggered, but the store is already merging!");
			return; // ignore
		}

		// check that the native store isn't empty
		if (!isNativeStoreContainsAtLeast(1)) {
			return;
		}

		logger.info("Merging..." + triplesCount);

		try {
			this.isMergeTriggered = true;
			logger.debug("START MERGE");
			mergerThread = mergeRunnable.createThread();
			mergerThread.start();
			logger.debug("MERGE THREAD LAUNCHED");
		} catch (Exception e) {
			throw new MergeStartException("Crash while starting the merge", e);
		}
	}

	// @todo: this can be dangerous, what if it is called 2 times, then two
	// threads will start which will overlap each
	// other, only one should be allowed, no?
	// should not be called from the outside because it's internals, the case is
	// handled in the EndpointStoreConnection
	// when
	// the store is being merged we don't call it again..
	// starts the merging process to merge the delta into HDT

	/**
	 * merge the store if required, would not do anything if
	 * {@link #getThreshold()} returns a non-positive number
	 */
	public void mergeIfRequired() {
		logger.debug("--------------: triplesCount=" + triplesCount);
		// Merge only if threshold in native store exceeded and not merging with
		// hdt
		if (getThreshold() >= 0 && triplesCount >= getThreshold()) {
			try {
				mergeStore(false);
			} catch (MergeStartException e) {
				e.printStackTrace();
				// ignore exception
			}
		}
	}

	/**
	 * test if the native store contains at least a certain number of triples
	 *
	 * @param number the number of triples to at least have
	 * @return true if the size of the store is at least number, false otherwise
	 */
	public boolean isNativeStoreContainsAtLeast(long number) {
		try {
			lockToPreventNewConnections.waitForActiveLocks();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		Lock lock = locksHoldByConnections.createLock("count-lock");

		try (SailConnection connection = getChangingStore().getConnection()) {
			// https://github.com/eclipse/rdf4j/discussions/3734
			// return connection.size() >= number;
			try (CloseableIteration<? extends Statement, SailException> it = connection.getStatements(null, null, null,
					false)) {
				for (long i = 0; i < number; i++) {
					if (!it.hasNext()) {
						return false;
					}
					it.next();
				}
				return true;
			}
		} finally {
			lock.release();
		}
	}

	public long countTriplesNativeStore() {
		try (SailConnection connectionA = getNativeStoreA().getConnection()) {
			try (SailConnection connectionB = getNativeStoreB().getConnection()) {
				return connectionA.size() + connectionB.size();
			}
		}
	}

	public void flushWrites() throws IOException {
		getDeleteBitMap().force(true);
		if (isMerging()) {
			getRdfWriterTempTriples().getWriter().flush();
			getTempDeleteBitMap().force(true);
		}
		logger.debug("Writes completed");
	}

	public HDTConverter getHdtConverter() {
		return hdtConverter;
	}

	public void setTriplesCount(long triplesCount) {
		this.triplesCount = triplesCount;
	}

	public BitArrayDisk getBitX() {
		return bitX;
	}

	public BitArrayDisk getBitY() {
		return bitY;
	}

	public BitArrayDisk getBitZ() {
		return bitZ;
	}

	public HDTSpecification getHDTSpec() {
		return spec;
	}

	public void setSpec(HDTSpecification spec) {
		this.spec = spec;
	}

	public MergeRunnable getMergeRunnable() {
		return mergeRunnable;
	}

	public EndpointFiles getEndpointFiles() {
		return endpointFiles;
	}

	public int getExtendsTimeMergeBeginning() {
		return MergeRunnable.getExtendsTimeMergeBeginning();
	}

	public int getExtendsTimeMergeBeginningAfterSwitch() {
		return MergeRunnable.getExtendsTimeMergeBeginningAfterSwitch();
	}

	public int getExtendsTimeMergeEnd() {
		return MergeRunnable.getExtendsTimeMergeEnd();
	}

	public void setExtendsTimeMergeBeginning(int extendsTimeMergeBeginning) {
		MergeRunnable.setExtendsTimeMergeBeginning(extendsTimeMergeBeginning);
	}

	public void setExtendsTimeMergeBeginningAfterSwitch(int extendsTimeMergeBeginningAfterSwitch) {
		MergeRunnable.setExtendsTimeMergeBeginningAfterSwitch(extendsTimeMergeBeginningAfterSwitch);
	}

	public void setExtendsTimeMergeEnd(int extendsTimeMergeEnd) {
		MergeRunnable.setExtendsTimeMergeEnd(extendsTimeMergeEnd);
	}

	public void setFreezeNotifications(boolean freezeNotifications) {
		this.freezeNotifications = freezeNotifications;
	}

	public boolean isNotificationsFreeze() {
		return freezeNotifications;
	}

	long getDebugId() {
		return debugId;
	}
}
