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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 */
public class HeapInternalTimerService<K, N> extends InternalTimerService<K, N> {

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private final Set<InternalTimer<K, N>>[] processingTimeTimersByKeyGroup;
	private final PriorityQueue<InternalTimer<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	private final Set<InternalTimer<K, N>>[] eventTimeTimersByKeyGroup;
	private final PriorityQueue<InternalTimer<K, N>> eventTimeTimersQueue;

	@SuppressWarnings("unchecked")
	HeapInternalTimerService(
		int totalKeyGroups,
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService) {

		super(totalKeyGroups, localKeyGroupRange, keyContext, processingTimeService);

		this.eventTimeTimersQueue = new PriorityQueue<>(100);
		this.eventTimeTimersByKeyGroup = new HashSet[keyGroupRange.getNumberOfKeyGroups()];

		this.processingTimeTimersQueue = new PriorityQueue<>(100);
		this.processingTimeTimersByKeyGroup = new HashSet[keyGroupRange.getNumberOfKeyGroups()];
	}

	@Override
	public void start() {
		if (processingTimeTimersQueue.size() > 0) {
			nextTimer = processingTimeService.registerTimer(processingTimeTimersQueue.peek().getTimestamp(), this);
		}
	}

	@Override
	public void close() {
		eventTimeTimersQueue.clear();
		for (int i = 0; i < eventTimeTimersByKeyGroup.length; ++i) {
			eventTimeTimersByKeyGroup[i] = null;
		}

		processingTimeTimersQueue.clear();
		for (int i = 0; i < eventTimeTimersByKeyGroup.length; ++i) {
			processingTimeTimersByKeyGroup[i] = null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);

		// make sure we only put one timer per key into the queue
		Set<InternalTimer<K, N>> timerSet = getProcessingTimeTimerSetForTimer(timer);
		if (timerSet.add(timer)) {

			InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;

			processingTimeTimersQueue.add(timer);

			// check if we need to re-schedule our timer to earlier
			if (time < nextTriggerTime) {
				if (nextTimer != null) {
					nextTimer.cancel(false);
				}
				nextTimer = processingTimeService.registerTimer(time, this);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		Set<InternalTimer<K, N>> timerSet = getEventTimeTimerSetForTimer(timer);
		if (timerSet.add(timer)) {
			eventTimeTimersQueue.add(timer);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		Set<InternalTimer<K, N>> timerSet = getProcessingTimeTimerSetForTimer(timer);
		if (timerSet.remove(timer)) {
			processingTimeTimersQueue.remove(timer);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		Set<InternalTimer<K, N>> timerSet = getEventTimeTimerSetForTimer(timer);
		if (timerSet.remove(timer)) {
			eventTimeTimersQueue.remove(timer);
		}
	}

	@Override
	public void onProcessingTime(long time) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		nextTimer = null;

		InternalTimer<K, N> timer;

		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {

			Set<InternalTimer<K, N>> timerSet = getProcessingTimeTimerSetForTimer(timer);

			timerSet.remove(timer);
			processingTimeTimersQueue.remove();

			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		if (timer != null) {
			if (nextTimer == null) {
				nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
			}
		}
	}

	@Override
	public void onEventTime(long time) throws Exception {
		InternalTimer<K, N> timer;

		while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {

			Set<InternalTimer<K, N>> timerSet = getEventTimeTimerSetForTimer(timer);
			timerSet.remove(timer);
			eventTimeTimersQueue.remove();

			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}

	@Override
	public void restoreEventTimeTimersForKeyGroup(int keyGroupIdx, Iterable<InternalTimer<K, N>> timers) {

		checkArgument(keyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		Set<InternalTimer<K, N>> eventTimers = getEventTimeTimersForKeyGroup(keyGroupIdx);
		for (InternalTimer<K, N> timer : timers) {
			eventTimers.add(timer);
			eventTimeTimersQueue.add(timer);
		}
	}

	@Override
	public void restoreProcessingTimeTimersForKeyGroup(int keyGroupIdx, Iterable<InternalTimer<K, N>> timers) {

		checkArgument(keyGroupRange.contains(keyGroupIdx),
				"Key Group " + keyGroupIdx + " does not belong to the local range.");

		Set<InternalTimer<K, N>> processingTimeTimers = getProcessingTimeTimersForKeyGroup(keyGroupIdx);
		for (InternalTimer<K, N> timer : timers) {
			processingTimeTimers.add(timer);
			processingTimeTimersQueue.add(timer);
		}
	}

	/**
	 * Retrieve the set of event time timers for the key-group this timer belongs to.
	 *
	 * @param timer the timer whose key-group we are searching.
	 * @return the set of registered timers for the key-group.
	 */
	private Set<InternalTimer<K, N>> getEventTimeTimerSetForTimer(InternalTimer<K, N> timer) {
		checkArgument(keyGroupRange != null, "The operator has not been initialized.");
		int keyGroupIdx = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), this.totalKeyGroups);
		return getEventTimeTimersForKeyGroup(keyGroupIdx);
	}

	/**
	 * Retrieve the set of event time timers for the requested key-group.
	 *
	 * @param keyGroupIdx the index of the key group we are interested in.
	 * @return the set of registered timers for the key-group.
	 */
	@Override
	public Set<InternalTimer<K, N>> getEventTimeTimersForKeyGroup(int keyGroupIdx) {
		int localIdx = getIndexForKeyGroup(keyGroupIdx);
		Set<InternalTimer<K, N>> timers = eventTimeTimersByKeyGroup[localIdx];
		if (timers == null) {
			timers = new HashSet<>();
			eventTimeTimersByKeyGroup[localIdx] = timers;
		}
		return timers;
	}

	/**
	 * Retrieve the set of processing time timers for the key-group this timer belongs to.
	 *
	 * @param timer the timer whose key-group we are searching.
	 * @return the set of registered timers for the key-group.
	 */
	private Set<InternalTimer<K, N>> getProcessingTimeTimerSetForTimer(InternalTimer<K, N> timer) {
		checkArgument(keyGroupRange != null, "The operator has not been initialized.");
		int keyGroupIdx = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), this.totalKeyGroups);
		return getProcessingTimeTimersForKeyGroup(keyGroupIdx);
	}

	/**
	 * Retrieve the set of processing time timers for the requested key-group.
	 *
	 * @param keyGroupIdx the index of the key group we are interested in.
	 * @return the set of registered timers for the key-group.
	 */
	@Override
	public Set<InternalTimer<K, N>> getProcessingTimeTimersForKeyGroup(int keyGroupIdx) {
		int localIdx = getIndexForKeyGroup(keyGroupIdx);
		Set<InternalTimer<K, N>> timers = processingTimeTimersByKeyGroup[localIdx];
		if (timers == null) {
			timers = new HashSet<>();
			processingTimeTimersByKeyGroup[localIdx] = timers;
		}
		return timers;
	}

	/**
	 * Computes the index of the requested key-group in the local datastructures.
	 * <li/>
	 * Currently we assume that each task is assigned a continuous range of key-groups,
	 * e.g. 1,2,3,4, and not 1,3,5. We leverage this to keep the different states by
	 * key-grouped in arrays instead of maps, where the offset for each key-group is
	 * the key-group id (an int) minus the id of the first key-group in the local range.
	 * This is for performance reasons.
	 */
	private int getIndexForKeyGroup(int keyGroupIdx) {
		checkArgument(keyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");
		return keyGroupIdx - this.keyGroupRange.getStartKeyGroup();
	}

	@VisibleForTesting
	@Override
	public int numProcessingTimeTimers() {
		return this.processingTimeTimersQueue.size();
	}

	@VisibleForTesting
	@Override
	public int numEventTimeTimers() {
		return this.eventTimeTimersQueue.size();
	}

	@VisibleForTesting
	@Override
	public int numProcessingTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimer<K, N> timer : processingTimeTimersQueue) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}
		return count;
	}

	@VisibleForTesting
	@Override
	public int numEventTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimer<K, N> timer : eventTimeTimersQueue) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}
		return count;
	}
}
