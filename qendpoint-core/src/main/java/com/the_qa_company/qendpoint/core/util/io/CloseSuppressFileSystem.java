package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

/**
 * {@link FileSystem} implementation for {@link CloseSuppressPath}
 *
 * @author Antoine Willerval
 */
public class CloseSuppressFileSystem extends FileSystem {
	private final FileSystem fileSystem;

	public CloseSuppressFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public FileSystemProvider provider() {
		return new CloseSuppressFileProvider(fileSystem.provider());
	}

	@Override
	public void close() throws IOException {
		fileSystem.close();
	}

	@Override
	public boolean isOpen() {
		return fileSystem.isOpen();
	}

	@Override
	public boolean isReadOnly() {
		return fileSystem.isReadOnly();
	}

	@Override
	public String getSeparator() {
		return fileSystem.getSeparator();
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		return CloseSuppressPath.of(fileSystem.getRootDirectories());
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		return fileSystem.getFileStores();
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		return fileSystem.supportedFileAttributeViews();
	}

	@Override
	public Path getPath(String first, String... more) {
		return CloseSuppressPath.of(fileSystem.getPath(first, more));
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		return fileSystem.getPathMatcher(syntaxAndPattern);
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		return fileSystem.getUserPrincipalLookupService();
	}

	@Override
	public WatchService newWatchService() throws IOException {
		return fileSystem.newWatchService();
	}
}
