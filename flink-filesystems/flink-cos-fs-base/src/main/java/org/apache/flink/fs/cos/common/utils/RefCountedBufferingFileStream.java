package org.apache.flink.fs.cos.common.utils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.function.FunctionWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class RefCountedBufferingFileStream extends RefCountedFSOutputStream {
	private static final Logger LOG = LoggerFactory.getLogger(RefCountedBufferingFileStream.class);

	public static final int BUFFER_SIZE = 4096;

	private final RefCountedFile currentTmpFile;

	/** The write buffer. */
	private final byte[] buffer;

	/** Current position in the buffer, must be in [0, buffer.length]. */
	private int positionInBuffer;

	private boolean closed;

	@VisibleForTesting
	public RefCountedBufferingFileStream(
		final RefCountedFile file,
		final int bufferSize) {

		checkArgument(bufferSize > 0L);

		this.currentTmpFile = checkNotNull(file);
		this.buffer = new byte[bufferSize];
		this.positionInBuffer = 0;
		this.closed = false;
	}

	@Override
	public File getInputFile() {
		return currentTmpFile.getFile();
	}

	@Override
	public long getPos() {
		return currentTmpFile.getLength() + positionInBuffer;
	}

	@Override
	public void write(int b) throws IOException {
		if (positionInBuffer >= buffer.length) {
			flush();
		}

		requireOpen();

		buffer[positionInBuffer++] = (byte) b;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (len >= buffer.length) {
			// circumvent the internal buffer for large writes
			flush();
			currentTmpFile.write(b, off, len);
			return;
		}

		requireOpen();

		if (len > buffer.length - positionInBuffer) {
			flush();
		}

		System.arraycopy(b, off, buffer, positionInBuffer, len);
		positionInBuffer += len;
	}

	@Override
	public void flush() throws IOException {
		LOG.info("begin to flush the buffer to the file: {}.", this.currentTmpFile);
		currentTmpFile.write(buffer, 0, positionInBuffer);
		currentTmpFile.flush();
		positionInBuffer = 0;
		LOG.info("finish to flush the buffer to the file: {}.", this.currentTmpFile);
	}

	@Override
	public void sync() throws IOException {
		throw new UnsupportedOperationException("COSRecoverableFsDataOutputStream cannot sync state to COS. " +
			"Use persist() to create a persistent recoverable intermediate point.");
	}

	@Override
	public boolean isClosed() throws IOException {
		return closed;
	}

	@Override
	public void close() {
		if (!closed) {
			LOG.info("begin to close the file: {}. ", this.currentTmpFile);
			currentTmpFile.closeStream();
			closed = true;
			LOG.info("end to close the file: {}.", this.currentTmpFile);
		}
	}

	@Override
	public void retain() {
		currentTmpFile.retain();
	}

	@Override
	public boolean release() {
		return currentTmpFile.release();
	}

	private void requireOpen() throws IOException {
		if (closed) {
			throw new IOException("Stream closed.");
		}
	}

	@Override
	public String toString() {
		return "Reference Counted File with {" +
			"path=\'" + currentTmpFile.getFile().toPath().toAbsolutePath() + "\'" +
			", size=" + getPos() +
			", reference counter=" + currentTmpFile.getReferenceCounter() +
			", closed=" + closed +
			'}';
	}

	@VisibleForTesting
	int getPositionInBuffer() {
		return positionInBuffer;
	}

	@VisibleForTesting
	public int getReferenceCounter() {
		return currentTmpFile.getReferenceCounter();
	}

	// ------------------------- Factory Methods -------------------------

	public static RefCountedBufferingFileStream openNew(
		final FunctionWithException<File, RefCountedFile, IOException> tmpFileProvider) throws IOException {

		return new RefCountedBufferingFileStream(
			tmpFileProvider.apply(null),
			BUFFER_SIZE);
	}

	public static RefCountedBufferingFileStream restore(
		final FunctionWithException<File, RefCountedFile, IOException> tmpFileProvider,
		final File initialTmpFile) throws IOException {

		return new RefCountedBufferingFileStream(
			tmpFileProvider.apply(initialTmpFile),
			BUFFER_SIZE);
	}
}
