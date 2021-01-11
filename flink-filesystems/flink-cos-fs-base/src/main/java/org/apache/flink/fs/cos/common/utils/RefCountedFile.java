package org.apache.flink.fs.cos.common.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RefCountedFile implements RefCounted {
	private final File file;
	private final OffsetTrackOutputStream outputStream;
	private final AtomicInteger references;
	private boolean closed;

	public static RefCountedFile newFile(
		final File file,
		final OutputStream currentOutputStream) {
		return new RefCountedFile(file, currentOutputStream, 0L);
	}

	public static RefCountedFile restoreFile(
		final File file,
		final OutputStream currentOutputStream,
		final long byteInCurrentPart) {
		return new RefCountedFile(file, currentOutputStream, byteInCurrentPart);
	}

	private RefCountedFile(
		final File file,
		final OutputStream currentOutputStream,
		final long bytesInCurrentPart) {

		this.file = checkNotNull(file);
		this.outputStream = new OffsetTrackOutputStream(
			currentOutputStream, bytesInCurrentPart);
		this.references = new AtomicInteger(1);
		this.closed = false;
	}

	public File getFile() {
		return file;
	}

	public OffsetTrackOutputStream getOutputStream() {
		return outputStream;
	}

	public long getLength() {
		return this.outputStream.getLength();
	}

	public void write(byte[] b, int off, int len) throws IOException {
		this.requiredOpened();
		if (len > 0) {
			this.outputStream.write(b, off, len);
		}
	}

	public void flush() throws IOException {
		this.requiredOpened();
		this.outputStream.flush();
	}

	public void closeStream() {
		if (!this.closed) {
			IOUtils.closeQuietly(this.outputStream);
			this.closed = true;
		}
	}

	private void requiredOpened() throws IOException {
		if (this.closed) {
			throw new IOException("The stream has been closed.");
		}
	}

	@Override
	public void retain() {
		this.references.incrementAndGet();
	}

	@Override
	public boolean release() {
		if (this.references.decrementAndGet() == 0) {
			return this.tryClose();
		}
		return false;
	}

	@VisibleForTesting
	int getReferenceCounter() {
		return references.get();
	}

	private boolean tryClose() {
		boolean deletedTag = false;
		try {
			deletedTag = Files.deleteIfExists(file.toPath());
		} catch (Throwable e) {
			ExceptionUtils.rethrowIfFatalError(e);
		}

		return deletedTag;
	}
}
