package org.apache.flink.fs.cos.common.utils;

import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.File;
import java.io.IOException;

/**
 * A {@link FSDataOutputStream} with the {@link RefCounted} functionality.
 */
public abstract class RefCountedFSOutputStream extends FSDataOutputStream implements RefCounted {
	public abstract File getInputFile();

	public abstract boolean isClosed() throws IOException;
}
