package org.apache.flink.fs.cos.common.writer;

import org.apache.flink.fs.cos.common.utils.RefCountedFSOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public interface RecoverableMultipartUpload {
	RecoverableFsDataOutputStream.Committer snapshotAndGetCommitter() throws IOException;

	RecoverableWriter.ResumeRecoverable snapshotAndGetRecoverable(
		@Nullable final RefCountedFSOutputStream incompletePartFile) throws IOException;

	void uploadPart(final RefCountedFSOutputStream file) throws IOException;

	Optional<File> getIncompletePart();
}
