package org.apache.flink.fs.cos.common.writer;

import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.PartETag;
import org.apache.hadoop.fs.FileMetadata;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public interface COSAccessHelper {
	/**
	 * Initialize a multipart upload.
	 * @param key the cos key whose value we want to upload in parts.
	 * @return	The upload id the initiated multi-part upload which will be used during the uploading of parts.
	 * @throws IOException
	 */
	String startMultipartUpload(String key) throws IOException;

	/**
	 * Uploads a part and associates it with MPU with the provided
	 * @param key The key which MPU is associated with
	 * @param uploadId the upload id of the MPU
	 * @param partNumber the part number of the part being uploaded.
	 * @param inputFile the file holding the part to be uploaded
	 * @return	The {@link PartETag} of the attempt to upload the part.
	 * @throws IOException
	 */
	PartETag uploadPart(String key, String uploadId, int partNumber,
						File inputFile, byte[] md5Hash) throws IOException;

	/**
	 *
	 * @param key The cos key used to identify this part.
	 * @param inputFile the local file holding the data to be uploaded.
	 * @throws IOException
	 */
	void putObject(String key, File inputFile, byte[] md5Hash) throws IOException;

	/**
	 *
	 * @param key The key identifying the object we finished uploading.
	 * @param uploadId the upload id of the multipart upload.
	 * @param partETags the list of {@link PartETag} associated with the Multipart Upload.
	 * @return the Complete Multipart upload result.
	 * @throws IOException
	 */
	CompleteMultipartUploadResult commitMultipartUpload(String key,
String uploadId, List<PartETag> partETags) throws IOException;

	boolean deleteObject(String key) throws IOException;

	long getObject(String key, File targetLocation) throws IOException;

	FileMetadata getObjectMetadata(String key) throws IOException;
}
