package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath.of;

/**
 * {@link FileSystemProvider} implementation for {@link CloseSuppressPath}
 *
 * @author Antoine Willerval
 */
public class CloseSuppressFileProvider extends FileSystemProvider {
	private final FileSystemProvider provider;

	public CloseSuppressFileProvider(FileSystemProvider provider) {
		this.provider = provider;
	}

	@Override
	public String getScheme() {
		return provider.getScheme();
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		return provider.newFileSystem(uri, env);
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		return provider.getFileSystem(uri);
	}

	@Override
	public Path getPath(URI uri) {
		return CloseSuppressPath.of(provider.getPath(uri));
	}

	@Override
	public FileSystem newFileSystem(Path path, Map<String, ?> env) throws IOException {
		return provider.newFileSystem(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), env);
	}

	@Override
	public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
		return provider.newInputStream(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), options);
	}

	@Override
	public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
		return provider.newOutputStream(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), options);
	}

	@Override
	public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		return provider.newFileChannel(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), options, attrs);
	}

	@Override
	public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options,
			ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
		return provider.newAsynchronousFileChannel(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), options,
				executor, attrs);
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		return provider.newByteChannel(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), options, attrs);
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
			throws IOException {
		return provider.newDirectoryStream(
				(dir instanceof CloseSuppressPath ? ((CloseSuppressPath) dir).getJavaPath() : dir), filter);
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		provider.createDirectory((dir instanceof CloseSuppressPath ? ((CloseSuppressPath) dir).getJavaPath() : dir),
				attrs);
	}

	@Override
	public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException {
		provider.createSymbolicLink(
				(link instanceof CloseSuppressPath ? ((CloseSuppressPath) link).getJavaPath() : link), target, attrs);
	}

	@Override
	public void createLink(Path link, Path existing) throws IOException {
		provider.createLink((link instanceof CloseSuppressPath ? ((CloseSuppressPath) link).getJavaPath() : link),
				existing);
	}

	@Override
	public void delete(Path path) throws IOException {
		provider.delete((path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path));
	}

	@Override
	public boolean deleteIfExists(Path path) throws IOException {
		return provider
				.deleteIfExists((path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path));
	}

	@Override
	public Path readSymbolicLink(Path link) throws IOException {
		return provider.readSymbolicLink(
				(link instanceof CloseSuppressPath ? ((CloseSuppressPath) link).getJavaPath() : link));
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		provider.copy((source instanceof CloseSuppressPath ? ((CloseSuppressPath) source).getJavaPath() : source),
				(target instanceof CloseSuppressPath ? ((CloseSuppressPath) target).getJavaPath() : target), options);
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		provider.move((source instanceof CloseSuppressPath ? ((CloseSuppressPath) source).getJavaPath() : source),
				(target instanceof CloseSuppressPath ? ((CloseSuppressPath) target).getJavaPath() : target), options);
	}

	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		return provider.isSameFile(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path),
				(path2 instanceof CloseSuppressPath ? ((CloseSuppressPath) path2).getJavaPath() : path2));
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		return provider.isHidden((path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path));
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		return provider
				.getFileStore((path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path));
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		provider.checkAccess((path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path),
				modes);
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		return provider.getFileAttributeView(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), type, options);
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
			throws IOException {
		return provider.readAttributes(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), type, options);
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		return provider.readAttributes(
				(path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path), attributes,
				options);
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		provider.setAttribute((path instanceof CloseSuppressPath ? ((CloseSuppressPath) path).getJavaPath() : path),
				attribute, value, options);
	}
}
