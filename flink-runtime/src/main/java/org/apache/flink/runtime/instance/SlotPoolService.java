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

package org.apache.flink.runtime.instance;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SlotOptions;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.clock.SystemClock;
import scala.concurrent.duration.Duration;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to hold all auxiliary services used by the {@link SlotPool}.
 */
public class SlotPoolService {

	private static final long SLOT_POOL_SHUTDOWN_TIMEOUT = 1000;

	private final TimerService<AllocatedSlot> timerService;

	private final Time slotAllocationTimeout;
	private final Time slotRequestResourceManagerTimeout;
	private final Time slotAllocationResourceManagerTimeout;
	private final Time slotIdleTimeout;

	public SlotPoolService(
			TimerService<AllocatedSlot> timerService,
			Time slotAllocationTimeout,
			Time slotRequestResourceManagerTimeout,
			Time slotAllocationResourceManagerTimeout,
			Time slotIdleTimeout) {

		this.timerService = checkNotNull(timerService);
		this.slotAllocationTimeout = checkNotNull(slotAllocationTimeout);
		this.slotRequestResourceManagerTimeout = checkNotNull(slotRequestResourceManagerTimeout);
		this.slotAllocationResourceManagerTimeout = checkNotNull(slotAllocationResourceManagerTimeout);
		this.slotIdleTimeout = checkNotNull(slotIdleTimeout);
	}

	public TimerService<AllocatedSlot> getTimerService() {
		return timerService;
	}

	public Time getSlotAllocationTimeout() {
		return slotAllocationTimeout;
	}

	public Time getSlotRequestResourceManagerTimeout() {
		return slotRequestResourceManagerTimeout;
	}

	public Time getSlotAllocationResourceManagerTimeout() {
		return slotAllocationResourceManagerTimeout;
	}

	public Time getSlotIdleTimeout() {
		return slotIdleTimeout;
	}

	public SlotPool createSlotPool(RpcService rpcService, JobID jobId) {
		return new SlotPool(
			rpcService,
			this.timerService,
			jobId,
			SystemClock.getInstance(),
			this.slotAllocationTimeout,
			this.slotAllocationResourceManagerTimeout,
			this.slotRequestResourceManagerTimeout,
			this.slotIdleTimeout);
	}

	public static SlotPoolService fromConfiguration(Configuration config) {
		checkNotNull(config);

		final String slotAllocationTimeoutDescription = config.getString(SlotOptions.SLOT_ALLOCATION_TIMEOUT);

		final Time slotAllocationTimeout;

		try {
			final Duration pause = Duration.create(slotAllocationTimeoutDescription);
			if (pause.isFinite()) {
				slotAllocationTimeout = Time.milliseconds(pause.toMillis());
			} else {
				throw new IllegalArgumentException("Slot allocation timeout should be finite.");
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				SlotOptions.SLOT_ALLOCATION_TIMEOUT.key() + ": " + slotAllocationTimeoutDescription);
		}

		final String slotRequestResourceManagerTimeoutDescription = config.getString(
			SlotOptions.SLOT_REQUEST_RESOURCE_MANAGER_TIMEOUT);

		final Time slotRequestResourceManagerTimeout;

		try {
			final Duration pause = Duration.create(slotRequestResourceManagerTimeoutDescription);
			if (pause.isFinite()) {
				slotRequestResourceManagerTimeout = Time.milliseconds(pause.toMillis());
			} else {
				throw new IllegalArgumentException("Slot request to resource manager should be finite.");
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				SlotOptions.SLOT_REQUEST_RESOURCE_MANAGER_TIMEOUT.key() + ": " +
				slotRequestResourceManagerTimeoutDescription);
		}

		final String slotAllocationResourceManagerTimeoutDescription = config.getString(
			SlotOptions.SLOT_ALLOCATION_RESOURCE_MANAGER_TIMEOUT);

		final Time slotAllocationResourceManagerTimeout;

		try {
			final Duration pause = Duration.create(slotAllocationResourceManagerTimeoutDescription);
			if (pause.isFinite()) {
				slotAllocationResourceManagerTimeout = Time.milliseconds(pause.toMillis());
			} else {
				throw new IllegalArgumentException("Slot allocation to resource manager timeout should be finite.");
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				SlotOptions.SLOT_REQUEST_RESOURCE_MANAGER_TIMEOUT.key() + ": " +
				slotAllocationResourceManagerTimeoutDescription);
		}

		final String slotIdleTimeoutDescription = config.getString(
			SlotOptions.SLOT_POOL_IDLE_TIMEOUT);

		final Time slotIdleTimeout;

		try {
			final Duration pause = Duration.create(slotIdleTimeoutDescription);
			if (pause.isFinite()) {
				slotIdleTimeout = Time.milliseconds(pause.toMillis());
			} else {
				throw new IllegalArgumentException("Slot pool idle timeout should be finite.");
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				SlotOptions.SLOT_POOL_IDLE_TIMEOUT.key() + ": " + slotIdleTimeoutDescription);
		}

		final TimerService<AllocatedSlot> timerService = new TimerService<>(
			new ScheduledThreadPoolExecutor(
				1,
				new ExecutorThreadFactory("slot-pool-timer-service")),
			SLOT_POOL_SHUTDOWN_TIMEOUT);

		return new SlotPoolService(
			timerService,
			slotAllocationTimeout,
			slotRequestResourceManagerTimeout,
			slotAllocationResourceManagerTimeout,
			slotIdleTimeout);
	}
}
