package org.apache.flink.fs.cos.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An executor decorate that allows only a certain number of concurrent executions.
 * The {@link #execute(Runnable)} method blocks once that number of executions is exceeded.
 */
@Internal
public final class BackPressuringExecutor implements Executor {
	private final Executor delegate;

	private final Semaphore permits;

	public BackPressuringExecutor(Executor delegate, int numConcurrentExecutions) {
		checkArgument(numConcurrentExecutions > 0, "numConcurrentExecutions must be > 0");
		this.delegate = checkNotNull(delegate, "delegate");
		this.permits = new Semaphore(numConcurrentExecutions, true);
	}

	@Override
	public void execute(Runnable command) {
		try {
			permits.acquire();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new FlinkRuntimeException("interrupted:", e);
		}

		final SemaphoreReleasingRunnable runnable = new SemaphoreReleasingRunnable(command, permits);
		try {
			delegate.execute(runnable);
		} catch (Throwable e) {
			runnable.release();
			ExceptionUtils.rethrow(e, e.getMessage());
		}
	}

	private static class SemaphoreReleasingRunnable implements Runnable {

		private final Runnable delegate;

		private final Semaphore toRelease;

		private final AtomicBoolean released = new AtomicBoolean();

		SemaphoreReleasingRunnable(Runnable delegate, Semaphore toRelease) {
			this.delegate = delegate;
			this.toRelease = toRelease;
		}

		@Override
		public void run() {
			try {
				delegate.run();
			}
			finally {
				release();
			}
		}

		void release() {
			if (released.compareAndSet(false, true)) {
				toRelease.release();
			}
		}
	}
}
