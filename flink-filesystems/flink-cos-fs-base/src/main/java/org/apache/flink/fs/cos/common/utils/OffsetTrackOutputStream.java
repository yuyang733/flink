package org.apache.flink.fs.cos.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * The {@link OutputStream} that keeps track of its current length;
 */
@Internal
public final class OffsetTrackOutputStream implements Closeable {
	private final OutputStream currentOut;
	private long position;

	public OffsetTrackOutputStream(OutputStream currentOut, long position) {
		this.currentOut = currentOut;
		this.position = position;
	}

	public long getLength() {
		return this.position;
	}

	public void write(byte[] b, int off, int len) throws IOException {
		this.currentOut.write(b, off, len);
		position += len;
	}

	public void flush() throws IOException {
		this.currentOut.flush();
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(this.currentOut);
	}
}
