package org.apache.flink.fs.cos.common.writer;

import com.qcloud.cos.model.PartETag;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class COSRecoverableSerializer implements SimpleVersionedSerializer<COSRecoverable> {
	static final COSRecoverableSerializer INSTANCE = new COSRecoverableSerializer();

	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private static final int MAGIC_NUMBER = 0x98761432;

	public COSRecoverableSerializer() {
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public byte[] serialize(COSRecoverable obj) throws IOException {
		final List<PartETag> partList = obj.getPartETags();
		final PartETag[] parts = partList.toArray(new PartETag[partList.size()]);

		final byte[] keyBytes = obj.getObjectName().getBytes(CHARSET);
		final byte[] uploadIdBytes = obj.getUploadId().getBytes(CHARSET);

		final byte[][] etags = new byte[parts.length][];
		int partEtagBytes = 0;
		for (int i = 0; i < parts.length; i++) {
			etags[i] = parts[i].getETag().getBytes(CHARSET);
			partEtagBytes += etags[i].length + 2 * Integer.BYTES;
		}

		final String lastObjectKey = obj.getInCompleteObjectName();
		final byte[] lastPartBytes = lastObjectKey == null ? null : lastObjectKey.getBytes(CHARSET);

		final byte[] targetBytes = new byte[
			Integer.BYTES + // magic number
				Integer.BYTES + keyBytes.length +
				Integer.BYTES + uploadIdBytes.length +
				Integer.BYTES + partEtagBytes +
				Long.BYTES +
				Integer.BYTES + (lastPartBytes == null ? 0 : lastPartBytes.length) +
				Long.BYTES
			];

		ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
		bb.putInt(MAGIC_NUMBER);

		bb.putInt(keyBytes.length);
		bb.put(keyBytes);

		bb.putInt(uploadIdBytes.length);
		bb.put(uploadIdBytes);

		bb.putInt(etags.length);
		for (int i = 0; i < parts.length; i++) {
			PartETag pe = parts[i];
			bb.putInt(pe.getPartNumber());
			bb.putInt(etags[i].length);
			bb.put(etags[i]);
		}

		bb.putLong(obj.getNumBytesInParts());

		if (lastPartBytes == null) {
			bb.putInt(0);
		} else {
			bb.putInt(lastPartBytes.length);
			bb.put(lastPartBytes);
		}

		bb.putLong(obj.getInCompleteObjectLength());

		return targetBytes;
	}

	@Override
	public COSRecoverable deserialize(int version, byte[] serialized) throws IOException {
		switch (version) {
			case 1:
				return deserializeV1(serialized);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	private static COSRecoverable deserializeV1(byte[] serialized) throws IOException {
		final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

		if (bb.getInt() != MAGIC_NUMBER) {
			throw new IOException("Corrupt data: Unexpected magic number.");
		}

		final byte[] keyBytes = new byte[bb.getInt()];
		bb.get(keyBytes);

		final byte[] uploadIdBytes = new byte[bb.getInt()];
		bb.get(uploadIdBytes);

		final int numParts = bb.getInt();
		final ArrayList<PartETag> parts = new ArrayList<>(numParts);
		for (int i = 0; i < numParts; i++) {
			final int partNum = bb.getInt();
			final byte[] buffer = new byte[bb.getInt()];
			bb.get(buffer);
			parts.add(new PartETag(partNum, new String(buffer, CHARSET)));
		}

		final long numBytes = bb.getLong();

		final String lastPart;
		final int lastObjectArraySize = bb.getInt();
		if (lastObjectArraySize == 0) {
			lastPart = null;
		} else {
			byte[] lastPartBytes = new byte[lastObjectArraySize];
			bb.get(lastPartBytes);
			lastPart = new String(lastPartBytes, CHARSET);
		}

		final long lastPartLength = bb.getLong();

		return new COSRecoverable(
			new String(uploadIdBytes, CHARSET),
			new String(keyBytes, CHARSET),
			parts,
			numBytes,
			lastPart,
			lastPartLength);
	}
}
