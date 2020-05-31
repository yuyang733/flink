package org.apache.flink.fs.cos.common.utils;

import org.apache.flink.util.function.FunctionWithException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;

public class RefCountedTmpFileCreator
	implements FunctionWithException<File, RefCountedFile, IOException> {

	private final File[] tempDirectories;

	private final AtomicInteger next;

	private RefCountedTmpFileCreator(File... tempDirectories) {
		checkArgument(tempDirectories.length > 0, "tempDirectories must not be empty");
		for (File f : tempDirectories) {
			if (f == null) {
				throw new IllegalArgumentException("tempDirectories contains null entries");
			}
		}

		this.tempDirectories = tempDirectories.clone();
		this.next = new AtomicInteger(new Random().nextInt(this.tempDirectories.length));
	}

	@Override
	public RefCountedFile apply(File file) throws IOException {
		final File directory = tempDirectories[nextIndex()];

		while (true) {
			try {
				if (file == null) {
					final File newFile = new File(directory, ".tmp_" + UUID.randomUUID());
					final OutputStream out = Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);
					return RefCountedFile.newFile(newFile, out);
				} else {
					final OutputStream out = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND);
					return RefCountedFile.restoreFile(file, out, file.length());
				}
			} catch (FileAlreadyExistsException ignored) {
				// fall through the loop and retry
			}
		}
	}

	private int nextIndex() {
		int currIndex, newIndex;
		do {
			currIndex = next.get();
			newIndex = currIndex + 1;
			if (newIndex == tempDirectories.length) {
				newIndex = 0;
			}
		} while (!next.compareAndSet(currIndex, newIndex));

		return currIndex;
	}

	public static RefCountedTmpFileCreator inDirectories(File... tmpDirectories) {
		return new RefCountedTmpFileCreator(tmpDirectories);
	}
}
