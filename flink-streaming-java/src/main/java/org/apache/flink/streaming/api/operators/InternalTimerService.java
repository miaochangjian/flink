/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.runtime.tasks.EventTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Interface for working with time and timers.
 *
 * <p>This is the internal version of {@link org.apache.flink.streaming.api.TimerService}
 * that allows to specify a key and a namespace to which timers should be scoped.
 *
 * All d
 * 
 * @param <K> Type of the keys in the stream
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public abstract class InternalTimerService<K, N> implements ProcessingTimeCallback, EventTimeCallback {

	protected final ProcessingTimeService processingTimeService;

	protected final KeyContext keyContext;

	protected final int totalKeyGroups;

	protected final KeyGroupRange keyGroupRange;

	/**
	 * The one and only Future (if any) registered to execute the
	 * next {@link Triggerable} action, when its (processing) time arrives.
	 */
	protected ScheduledFuture<?> nextTimer;

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	private long currentWatermark = Long.MIN_VALUE;

	// Variables to be set when the service is started.

	protected TypeSerializer<K> keySerializer;

	protected TypeSerializer<N> namespaceSerializer;

	private InternalTimer.TimerSerializer<K, N> timerSerializer;

	protected Triggerable<K, N> triggerTarget;

	private volatile boolean isInitialized;

	public InternalTimerService(
			int totalKeyGroups, 
			KeyGroupRange keyGroupRange, 
			KeyContext keyContext, 
			ProcessingTimeService processingTimeService) {
		
		this.totalKeyGroups = totalKeyGroups;
		this.keyGroupRange = checkNotNull(keyGroupRange);
		this.keyContext = checkNotNull(keyContext);
		this.processingTimeService = checkNotNull(processingTimeService);
	}

	/** Returns the current processing time. */
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	/** Returns the current event-time watermark. */
	public long currentWatermark() {
		return currentWatermark;
	}

	/**
	 * Registers a timer to be fired when processing time passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 */
	abstract public void registerProcessingTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	abstract public void deleteProcessingTimeTimer(N namespace, long time);

	/**
	 * Registers a timer to be fired when processing time passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 */
	abstract public void registerEventTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	abstract public void deleteEventTimeTimer(N namespace, long time);

	/**
	 * Returns the timers for the given key group.
	 */
	abstract public Set<InternalTimer<K, N>> getEventTimeTimersForKeyGroup(int keyGroup);

	/**
	 * Returns the timers for the given key group.
	 */
	abstract public Set<InternalTimer<K, N>> getProcessingTimeTimersForKeyGroup(int keyGroup);

	/**
	 * Restores the timers for the given key group.
	 */
	abstract public void restoreEventTimeTimersForKeyGroup(int keyGroup, Iterable<InternalTimer<K, N>> timers);

	/**
	 * Restores the timers for the given key group.
	 */
	abstract public void restoreProcessingTimeTimersForKeyGroup(int keyGroup, Iterable<InternalTimer<K, N>> timers);

	/**
	 * Starts the execution of the timer service
	 */
	abstract public void start();

	/**
	 * Closes the timer service.
	 */
	abstract public void close();
	
	public void advanceWatermark(long watermark) throws Exception {
		if (watermark < currentWatermark) {
			throw new IllegalStateException("The watermark is late.");
		}
		
		currentWatermark = watermark;
		
		onEventTime(watermark);
	}

	/**
	 * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 * @param stream the stream to write to.
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 */
	public void snapshotTimersForKeyGroup(DataOutputViewStreamWrapper stream, int keyGroupIdx) throws Exception {
		InstantiationUtil.serializeObject(stream, keySerializer);
		InstantiationUtil.serializeObject(stream, namespaceSerializer);

		// write the event time timers
		Collection<InternalTimer<K, N>> eventTimers = getEventTimeTimersForKeyGroup(keyGroupIdx);
		if (eventTimers != null) {
			stream.writeInt(eventTimers.size());
			for (InternalTimer<K, N> timer : eventTimers) {
				this.timerSerializer.serialize(timer, stream);
			}
		} else {
			stream.writeInt(0);
		}

		// write the processing time timers
		Collection<InternalTimer<K, N>> processingTimers = getProcessingTimeTimersForKeyGroup(keyGroupIdx);
		if (processingTimers != null) {
			stream.writeInt(processingTimers.size());
			for (InternalTimer<K, N> timer : processingTimers) {
				this.timerSerializer.serialize(timer, stream);
			}
		} else {
			stream.writeInt(0);
		}
	}

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 * @param stream the stream to read from.
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @param userCodeClassLoader the class loader that will be used to deserialize
	 * 								the local key and namespace serializers.
	 */
	public void restoreTimersForKeyGroup(DataInputViewStreamWrapper stream, int keyGroupIdx, ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {
		TypeSerializer<K> tmpKeySerializer = InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
		TypeSerializer<N> tmpNamespaceSerializer = InstantiationUtil.deserializeObject(stream, userCodeClassLoader);

		if ((this.keySerializer != null && !this.keySerializer.equals(tmpKeySerializer)) ||
					(this.namespaceSerializer != null && !this.namespaceSerializer.equals(tmpNamespaceSerializer))) {

				throw new IllegalArgumentException("Tried to restore timers " +
						"for the same service with different serializers.");
		}

		this.keySerializer = tmpKeySerializer;
		this.namespaceSerializer = tmpNamespaceSerializer;

		InternalTimer.TimerSerializer<K, N> timerSerializer =
				new InternalTimer.TimerSerializer<>(this.keySerializer, this.namespaceSerializer);

		checkArgument(keyGroupRange.contains(keyGroupIdx),
				"Key Group " + keyGroupIdx + " does not belong to the local range.");

		// read the event time timers
		int sizeOfEventTimeTimers = stream.readInt();
		if (sizeOfEventTimeTimers > 0) {
			List<InternalTimer<K, N>> eventTimeTimers = new ArrayList<>();
			for (int i = 0; i < sizeOfEventTimeTimers; i++) {
				InternalTimer<K, N> timer = timerSerializer.deserialize(stream);
				
				eventTimeTimers.add(timer);
			}

			restoreEventTimeTimersForKeyGroup(keyGroupIdx, eventTimeTimers);
		}

		// read the processing time timers
		int sizeOfProcessingTimeTimers = stream.readInt();
		if (sizeOfProcessingTimeTimers > 0) {
			List<InternalTimer<K, N>> processingTimeTimers = new ArrayList<>();
			for (int i = 0; i < sizeOfProcessingTimeTimers; i++) {
				InternalTimer<K, N> timer = timerSerializer.deserialize(stream);
				processingTimeTimers.add(timer);
			}

			restoreProcessingTimeTimersForKeyGroup(keyGroupIdx, processingTimeTimers);
		}
	}

	/**
	 * Starts the local {@link InternalTimerService} by:
	 * <ol>
	 *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
	 *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
	 *     <li>Re-registering timers that were retrieved after recoveting from a node failure, if any.</li>
	 * </ol>
	 * This method can be called multiple times, as long as it is called with the same serializers.
	 */
	void startTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerTarget)
	{

		if (isInitialized) {
			if (!(this.keySerializer.equals(keySerializer) && this.namespaceSerializer.equals(namespaceSerializer))) {
				throw new IllegalArgumentException("Already initialized Timer Service " +
						"tried to be initialized with different key and namespace serializers.");
			}
		} else {

			if (keySerializer == null || namespaceSerializer == null) {
				throw new IllegalArgumentException("The TimersService serializers cannot be null.");
			}

			if (this.triggerTarget != null) {
				throw new IllegalStateException("The TimerService has already been initialized.");
			}

			// the following is the case where we restore
			if ((this.keySerializer != null && !this.keySerializer.equals(keySerializer)) ||
					(this.namespaceSerializer != null && !this.namespaceSerializer.equals(namespaceSerializer))) {
				throw new IllegalStateException("Tried to initialize restored TimerService " +
						"with different serializers than those used to snapshot its state.");
			}

			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;

			this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

			this.timerSerializer = new InternalTimer.TimerSerializer<>(this.keySerializer, this.namespaceSerializer);

			start();

			this.isInitialized = true;
		}
	}

	@VisibleForTesting
	public int getLocalKeyGroupRangeStartIdx() {
		return this.keyGroupRange.getStartKeyGroup();
	}

	@VisibleForTesting
	public abstract int numProcessingTimeTimers();

	@VisibleForTesting
	public abstract int numEventTimeTimers();

	@VisibleForTesting
	public abstract int numProcessingTimeTimers(N namespace);

	@VisibleForTesting
	public abstract int numEventTimeTimers(N namespace);
}
