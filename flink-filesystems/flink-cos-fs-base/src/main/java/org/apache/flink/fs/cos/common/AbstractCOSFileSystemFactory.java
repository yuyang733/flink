package org.apache.flink.fs.cos.common;

import org.apache.flink.fs.cos.common.writer.COSAccessHelper;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.NativeFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * The base class for file system factories that create COS file systems.
 */
public abstract class AbstractCOSFileSystemFactory implements FileSystemFactory {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractCOSFileSystemFactory.class);

	private static final ConfigOption<Long> UPLOAD_PART_MIN_SIZE = ConfigOptions
		.key("cos.upload.part.min.size")
		.defaultValue(FlinkCOSFileSystem.COS_MULTIPART_UPLOAD_PART_MIN_SIZE)
		.withDescription("" +
			"This option is relevant to the Recoverable Writer and sets the min size of data that " +
			"buffered locally before being sent to the COS. This value is limited to the range: 1MB to 5GB.");

	public static final ConfigOption<Integer> MAX_CONCURRENT_UPLOADS = ConfigOptions
		.key("cos.upload.max.concurrent.uploads")
		.defaultValue(Runtime.getRuntime().availableProcessors())
		.withDescription(
			"This option is relevant to the Recoverable Writer and limits the number of " +
				"parts that can be concurrently in-flight. By default, this is set to " +
				Runtime.getRuntime().availableProcessors() + "."
		);

	// The name of the actual file system.
	private final String name;

	private Configuration flinkConfiguration;

	public AbstractCOSFileSystemFactory(String name) {
		this.name = name;
	}

	@Override
	public void configure(Configuration config) {
		this.flinkConfiguration = config;
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		Configuration flinkConfig = this.flinkConfiguration;

		if (flinkConfig == null) {
			LOG.warn("Creating S3 FileSystem without configuring the factory. All behavior will be default.");
			flinkConfig = new Configuration();
		}

		LOG.info("Creating the COS FileSystem backed by {}.", this.name);
		try {
			org.apache.hadoop.conf.Configuration hadoopConfiguration = this.getHadoopConfiguration();
			org.apache.hadoop.fs.FileSystem fs = createHadoopFileSystem();
			fs.initialize(getInitURI(fsUri, hadoopConfiguration), hadoopConfiguration);

			final String[] localTempDirectories
				= ConfigurationUtils.parseTempDirectories(flinkConfiguration);
			Preconditions.checkArgument(localTempDirectories.length > 0);
			final String localTempDirectory = localTempDirectories[0];
			final long cosMinPartSize = flinkConfig.getLong(UPLOAD_PART_MIN_SIZE);
			final int maxConcurrentUploads = flinkConfig.getInteger(MAX_CONCURRENT_UPLOADS);
			final COSAccessHelper cosAccessHelper = getCosAccessHelper(((CosFileSystem) fs).getStore());

			return new FlinkCOSFileSystem(
				fs,
				localTempDirectory,
				cosAccessHelper,
				cosMinPartSize,
				maxConcurrentUploads
			);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	protected abstract org.apache.hadoop.fs.FileSystem createHadoopFileSystem();

	protected abstract URI getInitURI(
		URI fsUri, org.apache.hadoop.conf.Configuration hadoopConfig);

	protected abstract COSAccessHelper getCosAccessHelper(NativeFileSystemStore nativeFileSystemStore);

	protected abstract org.apache.hadoop.conf.Configuration getHadoopConfiguration();
}
