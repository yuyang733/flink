package org.apache.flink.fs.cos.common;

import org.apache.flink.fs.cos.common.utils.RefCountedFile;
import org.apache.flink.fs.cos.common.utils.RefCountedTmpFileCreator;
import org.apache.flink.fs.cos.common.writer.COSAccessHelper;
import org.apache.flink.fs.cos.common.writer.COSRecoverableWriter;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class FlinkCOSFileSystem extends HadoopFileSystem {

	public static final long COS_MULTIPART_UPLOAD_PART_MIN_SIZE = 1 * 1024 * 1024;

	private final String localTmpDir;

	private final FunctionWithException<File, RefCountedFile, IOException> tmpFileCreator;

	private final COSAccessHelper cosAccessHelper;

	private final Executor uploadThreadPool;

	private final long cosUploadPartSize;

	private final int maxConcurrentUploadsPerStream;

	/**
	 * Wraps the given Hadoop File System object as a Flink File System object.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
	 */
	public FlinkCOSFileSystem(
		FileSystem hadoopFileSystem,
		String localTmpDir,
		COSAccessHelper cosAccessHelper,
		long cosUploadPartSize,
		int maxConcurrentUploadsPerStream) {
		super(hadoopFileSystem);
		this.localTmpDir = Preconditions.checkNotNull(localTmpDir);
		this.tmpFileCreator = RefCountedTmpFileCreator.inDirectories(new File(localTmpDir));
		this.cosAccessHelper = cosAccessHelper;
		this.uploadThreadPool = Executors.newCachedThreadPool();
		this.cosUploadPartSize = cosUploadPartSize;
		this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
	}

	public String getLocalTmpDir() {
		return localTmpDir;
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.OBJECT_STORE;
	}

	@Override
	public RecoverableWriter createRecoverableWriter() throws IOException {
		if (null == this.cosAccessHelper) {
			throw new UnsupportedOperationException("This cos file system implementation does not support recoverable writers.");
		}

		return COSRecoverableWriter.writer(getHadoopFileSystem(), this.tmpFileCreator, cosAccessHelper, this.uploadThreadPool, cosUploadPartSize, maxConcurrentUploadsPerStream);
	}
}
