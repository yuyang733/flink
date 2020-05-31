package org.apache.flink.fs.cos.common.writer;

import com.qcloud.cos.model.PartETag;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MultipartUploadInfo {

	private final String objectName;

	private final String uploadId;

	private final List<PartETag> completeParts;

	private final Optional<File> incompletePart;

	/**
	 * This contains both the parts that are already uploaded but also
	 * the ones that are queued to be uploaded at the {@link }.
	 */
	private int numberOfRegisteredParts;

	/**
	 * This is the total size of the upload, i.e. also with the parts
	 * that are queued but not uploaded yet.
	 */
	private long expectedSizeInBytes;

	MultipartUploadInfo(
		final String objectName,
		final String uploadId,
		final List<PartETag> completeParts,
		final long numBytes,
		final Optional<File> incompletePart) {

		checkArgument(numBytes >= 0L);

		this.objectName = checkNotNull(objectName);
		this.uploadId = checkNotNull(uploadId);
		this.completeParts = checkNotNull(completeParts);
		this.incompletePart = checkNotNull(incompletePart);

		this.numberOfRegisteredParts = completeParts.size();
		this.expectedSizeInBytes = numBytes;
	}

	String getObjectName() {
		return objectName;
	}

	String getUploadId() {
		return uploadId;
	}

	int getNumberOfRegisteredParts() {
		return numberOfRegisteredParts;
	}

	long getExpectedSizeInBytes() {
		return expectedSizeInBytes;
	}

	Optional<File> getIncompletePart() {
		return incompletePart;
	}

	List<PartETag> getCopyOfEtagsOfCompleteParts() {
		return new ArrayList<>(completeParts);
	}

	void registerNewPart(long length) {
		this.expectedSizeInBytes += length;
		this.numberOfRegisteredParts++;
	}

	void registerCompletePart(PartETag eTag) {
		completeParts.add(eTag);
	}

	int getRemainingParts() {
		return numberOfRegisteredParts - completeParts.size();
	}
}
