package com.the_qa_company.qendpoint.core.util.nsd;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.storage.QEPCoreException;
import org.spf4j.io.BufferedInputStream;
import org.spf4j.io.BufferedOutputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

import static java.lang.String.format;

/**
 * Thread safe namespace store backed up on disk, {@link #load()} operation load
 * the namespace, {@link #save(boolean)} save the state on disk.
 *
 * @author Antoine Willerval
 */
public class NamespaceData {
	private static final Logger logger = LoggerFactory.getLogger(NamespaceData.class);
	private static final ServiceLoader<NSDBinaryReader> LOADER = ServiceLoader.load(NSDBinaryReader.class);
	/**
	 * Current version of the NSD file
	 */
	public static final byte NS_VERSION = 0x10;
	public static final byte[] MAGIC = { '$', 'Q', 'E', 'P', 'N', 'S', 'D' };
	private static final Map<Byte, NSDBinaryReader> READERS = new HashMap<>();

	static {
		// each time the NS_VERSION is updated, a new reader should be added
		LOADER.stream().forEach(provider -> {
			NSDBinaryReader reader = provider.get();
			byte version = reader.version();
			NSDBinaryReader old = READERS.put(version, reader);
			if (old != null) {
				logger.warn("Found at least 2 NSDBinaryReader implementations with version {}", version);
			}
		});

		if (READERS.get(NS_VERSION) == null) {
			StringBuilder b = new StringBuilder();
			b.append(format("Can't find NSD implementation for current version %x\nAvailable: ", NS_VERSION));

			if (READERS.isEmpty()) {
				b.append("none");
			}
			for (NSDBinaryReader r : READERS.values()) {
				b.append(format("\n%x - %s", r.version(), r.getClass()));
			}

			throw new Error(b.toString());
		}
	}

	private final Path location;
	final Map<String, String> namespaces = new HashMap<>();
	private boolean updated;
	private final Object syncObject = new Object() {};

	public NamespaceData(Path location) {
		this.location = location;
	}

	/**
	 * set a namespace
	 *
	 * @param ns  namespace
	 * @param uri ns uri
	 */
	public void setNamespace(String ns, String uri) {
		synchronized (syncObject) {
			if (uri == null) {
				updated = updated || namespaces.remove(ns) != null;
			} else {
				String old = namespaces.put(ns, uri);
				updated = updated || !Objects.equals(old, uri);
			}
		}
	}

	/**
	 * remove a namespace
	 *
	 * @param ns namespace
	 */
	public void removeNamespace(String ns) {
		setNamespace(ns, null);
	}

	/**
	 * get the uri from a namespace
	 *
	 * @param ns namespace
	 * @return uri or null if this ns isn't associated with an uri
	 */
	public String getNamespace(String ns) {
		synchronized (syncObject) {
			return namespaces.get(ns);
		}
	}

	/**
	 * @return a copy of the namespaces
	 */
	public Map<String, String> getNamespaces() {
		synchronized (syncObject) {
			// clone the map
			return new HashMap<>(namespaces);
		}
	}

	/**
	 * save the namespace if an updated was made
	 *
	 * @throws QEPCoreException save exception
	 */
	public void sync() throws QEPCoreException {
		save(true);
	}

	/**
	 * clear previous data and load the namespace from disk
	 *
	 * @throws QEPCoreException load exception
	 */
	public void load() throws QEPCoreException {
		synchronized (syncObject) {
			try (InputStream is = new BufferedInputStream(Files.newInputStream(location))) {
				// check the binary magic
				byte[] header = is.readNBytes(MAGIC.length + 1);
				if (header.length < MAGIC.length + 1) {
					throw new EOFException("Can't read namespace data magic!");
				}

				for (int i = 0; i < MAGIC.length; i++) {
					if (header[i] != MAGIC[i]) {
						throw new IOException("Bad magic header!");
					}
				}

				// read the file version and search for a reader
				byte version = header[MAGIC.length];
				NSDBinaryReader reader = READERS.get(version);

				if (reader == null) {
					String versionStr = Integer.toString(Byte.toUnsignedInt(version));
					String currentVersionStr = Integer.toString(Byte.toUnsignedInt(NS_VERSION));
					if (version < NS_VERSION) {
						throw new IOException(
								format("Unknown version: %s, current: %s", versionStr, currentVersionStr));
					} else {
						throw new IOException(
								format("This namespace data was created with an newest version: %s, current: %s",
										versionStr, currentVersionStr));
					}
				}

				reader.readData(this, is, ProgressListener.ignore());
			} catch (NoSuchFileException ignore) {
				// we don't have any config, we can simply clear the namespaces
				namespaces.clear();
				updated = true;
			} catch (IOException e) {
				throw new QEPCoreException(e);
			}
		}
	}

	/**
	 * save the namespace on disk
	 *
	 * @param onlyIfUpdated only save if an update was made
	 * @throws QEPCoreException save exception
	 */
	public void save(boolean onlyIfUpdated) throws QEPCoreException {
		synchronized (syncObject) {
			if (onlyIfUpdated && !updated) {
				return; // not updated
			}
			ProgressListener pl = ProgressListener.ignore();
			try (OutputStream osh = new BufferedOutputStream(Files.newOutputStream(location))) {
				// write magic and version
				osh.write(MAGIC);
				osh.write(NS_VERSION);

				// write the file using the current version
				CRCOutputStream os = new CRCOutputStream(osh, new CRC32());
				VByte.encode(os, namespaces.size());

				for (Map.Entry<String, String> e : namespaces.entrySet()) {
					IOUtil.writeSizedString(os, e.getKey(), pl);
					IOUtil.writeSizedString(os, e.getValue(), pl);
				}

				os.writeCRC();
			} catch (IOException e) {
				throw new QEPCoreException(e);
			}
		}
	}

	public void clear() {
		synchronized (namespaces) {
			updated = updated || !namespaces.isEmpty();
			namespaces.clear();
		}
	}
}
