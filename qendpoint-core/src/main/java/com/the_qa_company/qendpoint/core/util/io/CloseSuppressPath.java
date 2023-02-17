package com.the_qa_company.qendpoint.core.util.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * a file that delete itself when we close it
 *
 * @author Antoine Willerval
 */
public class CloseSuppressPath implements Path, Closeable {
	public static final int BUFFER_SIZE = 1 << 13;
	private final Path wrapper;
	private boolean isDir;

	CloseSuppressPath(Path wrapper) {
		this.wrapper = wrapper;
	}

	public static CloseSuppressPath of(String first, String... more) {
		return new CloseSuppressPath(Path.of(first, more));
	}

	public static CloseSuppressPath of(Path component) {
		return component instanceof CloseSuppressPath ? (CloseSuppressPath) component
				: new CloseSuppressPath(component);
	}

	private static Path extract(Path other) {
		return (other instanceof CloseSuppressPath ? ((CloseSuppressPath) other).getJavaPath() : other);
	}

	public static Iterable<Path> of(Iterable<Path> component) {
		return () -> of(component.iterator());
	}

	public static Iterator<Path> of(Iterator<Path> it) {
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public CloseSuppressPath next() {
				return of(it.next());
			}

			@Override
			public void remove() {
				it.remove();
			}

			@Override
			public void forEachRemaining(Consumer<? super Path> action) {
				it.forEachRemaining(p -> action.accept(of(p)));
			}
		};
	}

	public static Path unpack(Path other) {
		return other instanceof CloseSuppressPath ? unpack(((CloseSuppressPath) other).getJavaPath()) : other;
	}

	@Override
	public FileSystem getFileSystem() {
		return new CloseSuppressFileSystem(wrapper.getFileSystem());
	}

	@Override
	public boolean isAbsolute() {
		return wrapper.isAbsolute();
	}

	@Override
	public CloseSuppressPath getRoot() {
		return of(wrapper.getRoot());
	}

	@Override
	public CloseSuppressPath getFileName() {
		return of(wrapper.getFileName());
	}

	@Override
	public CloseSuppressPath getParent() {
		return of(wrapper.getParent());
	}

	@Override
	public int getNameCount() {
		return wrapper.getNameCount();
	}

	@Override
	public CloseSuppressPath getName(int index) {
		return of(wrapper.getName(index));
	}

	@Override
	public CloseSuppressPath subpath(int beginIndex, int endIndex) {
		return of(wrapper.subpath(beginIndex, endIndex));
	}

	@Override
	public boolean startsWith(Path other) {
		return wrapper.startsWith(extract(other));
	}

	@Override
	public boolean startsWith(String other) {
		return wrapper.startsWith(other);
	}

	@Override
	public boolean endsWith(Path other) {
		return wrapper.endsWith(extract(other));
	}

	@Override
	public boolean endsWith(String other) {
		return wrapper.endsWith(other);
	}

	@Override
	public CloseSuppressPath normalize() {
		return of(wrapper.normalize());
	}

	@Override
	public CloseSuppressPath resolve(Path other) {
		return of(wrapper.resolve(extract(other)));
	}

	@Override
	public CloseSuppressPath resolve(String other) {
		return of(wrapper.resolve(other));
	}

	@Override
	public CloseSuppressPath resolveSibling(Path other) {
		return of(wrapper.resolveSibling(extract(other)));
	}

	@Override
	public CloseSuppressPath resolveSibling(String other) {
		return of(wrapper.resolveSibling(other));
	}

	@Override
	public CloseSuppressPath relativize(Path other) {
		return of(wrapper.relativize(extract(other)));
	}

	@Override
	public URI toUri() {
		return wrapper.toUri();
	}

	@Override
	public CloseSuppressPath toAbsolutePath() {
		return of(wrapper.toAbsolutePath());
	}

	@Override
	public Path toRealPath(LinkOption... options) throws IOException {
		return wrapper.toRealPath(options);
	}

	@Override
	public File toFile() {
		return wrapper.toFile();
	}

	@Override
	public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers)
			throws IOException {
		return wrapper.register(watcher, events, modifiers);
	}

	@Override
	public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
		return wrapper.register(watcher, events);
	}

	@Override
	public Iterator<Path> iterator() {
		return of(wrapper.iterator());
	}

	@Override
	public int compareTo(Path other) {
		return wrapper.compareTo(extract(other));
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof CloseSuppressPath) {
			return wrapper.equals(((CloseSuppressPath) other).getJavaPath());
		}
		return wrapper.equals(other);
	}

	@Override
	public int hashCode() {
		return wrapper.hashCode();
	}

	@Override
	public String toString() {
		return wrapper.toString();
	}

	@Override
	public void forEach(Consumer<? super Path> action) {
		wrapper.forEach(action);
	}

	@Override
	public Spliterator<Path> spliterator() {
		return wrapper.spliterator();
	}

	public InputStream openInputStream(int bufferSize, OpenOption... options) throws IOException {
		return new BufferedInputStream(openInputStream(options), bufferSize);
	}

	public InputStream openInputStream(OpenOption... options) throws IOException {
		return Files.newInputStream(this, options);
	}

	private OutputStream openOutputStream(OpenOption... options) throws IOException {
		return Files.newOutputStream(this, options);
	}

	public OutputStream openOutputStream(int bufferSize, OpenOption... options) throws IOException {
		return new BufferedOutputStream(openOutputStream(options), bufferSize);
	}

	/**
	 * close this path with a delete recurse instead of delete if exists
	 */
	public void closeWithDeleteRecurse() {
		isDir = true;
	}

	public void mkdirs() throws IOException {
		Files.createDirectories(this);
	}

	public Path getJavaPath() {
		return wrapper;
	}

	@Override
	public void close() throws IOException {
		if (isDir) {
			IOUtil.deleteDirRecurse(this);
		} else {
			Files.deleteIfExists(this);
		}
	}
}
