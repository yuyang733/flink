package org.apache.flink.fs.cos.common.utils;

import org.apache.flink.annotation.Internal;

@Internal
public interface RefCounted {
	void retain();

	boolean release();
}
