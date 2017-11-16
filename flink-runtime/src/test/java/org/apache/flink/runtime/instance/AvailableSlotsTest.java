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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotAndLocality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class AvailableSlotsTest {

	static final ResourceProfile DEFAULT_TESTING_PROFILE = new ResourceProfile(1.0, 512);

	static final ResourceProfile DEFAULT_TESTING_BIG_PROFILE = new ResourceProfile(2.0, 1024);

	@Test
	public void testAddAndRemove() throws Exception {
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots(null, null);

		final ResourceID resource1 = new ResourceID("resource1");
		final ResourceID resource2 = new ResourceID("resource2");

		final AllocatedSlot slot1 = createAllocatedSlot(resource1);
		final AllocatedSlot slot2 = createAllocatedSlot(resource1);
		final AllocatedSlot slot3 = createAllocatedSlot(resource2);

		availableSlots.add(slot1, 1L);
		availableSlots.add(slot2, 2L);
		availableSlots.add(slot3, 3L);

		assertEquals(3, availableSlots.size());
		assertTrue(availableSlots.contains(slot1.getSlotAllocationId()));
		assertTrue(availableSlots.contains(slot2.getSlotAllocationId()));
		assertTrue(availableSlots.contains(slot3.getSlotAllocationId()));
		assertTrue(availableSlots.containsTaskManager(resource1));
		assertTrue(availableSlots.containsTaskManager(resource2));

		availableSlots.removeAllForTaskManager(resource1);

		assertEquals(1, availableSlots.size());
		assertFalse(availableSlots.contains(slot1.getSlotAllocationId()));
		assertFalse(availableSlots.contains(slot2.getSlotAllocationId()));
		assertTrue(availableSlots.contains(slot3.getSlotAllocationId()));
		assertFalse(availableSlots.containsTaskManager(resource1));
		assertTrue(availableSlots.containsTaskManager(resource2));

		availableSlots.removeAllForTaskManager(resource2);

		assertEquals(0, availableSlots.size());
		assertFalse(availableSlots.contains(slot1.getSlotAllocationId()));
		assertFalse(availableSlots.contains(slot2.getSlotAllocationId()));
		assertFalse(availableSlots.contains(slot3.getSlotAllocationId()));
		assertFalse(availableSlots.containsTaskManager(resource1));
		assertFalse(availableSlots.containsTaskManager(resource2));
	}

	@Test
	public void testPollFreeSlot() {
		final SlotPoolService slotPoolService = SlotPoolService.fromConfiguration(new Configuration());
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots(
			slotPoolService.getTimerService(), slotPoolService.getSlotIdleTimeout());

		final ResourceID resource1 = new ResourceID("resource1");
		final AllocatedSlot slot1 = createAllocatedSlot(resource1);

		availableSlots.add(slot1, 1L);

		assertEquals(1, availableSlots.size());
		assertTrue(availableSlots.contains(slot1.getSlotAllocationId()));
		assertTrue(availableSlots.containsTaskManager(resource1));

		assertNull(availableSlots.poll(DEFAULT_TESTING_BIG_PROFILE, null));

		SlotAndLocality slotAndLocality = availableSlots.poll(DEFAULT_TESTING_PROFILE, null);
		assertEquals(slot1, slotAndLocality.slot());
		assertEquals(0, availableSlots.size());
		assertFalse(availableSlots.contains(slot1.getSlotAllocationId()));
		assertFalse(availableSlots.containsTaskManager(resource1));
	}

	@Test
	public void testAddAndRemoveWithTimerService() {
		@SuppressWarnings("unchecked")
		final TimerService<AllocatedSlot> timerService = Mockito.mock(TimerService.class);

		final SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots(timerService, Time.milliseconds(10));

		final ResourceID resource1 = new ResourceID("resource1");
		final ResourceID resource2 = new ResourceID("resource2");

		final AllocatedSlot slot1 = createAllocatedSlot(resource1);
		final AllocatedSlot slot2 = createAllocatedSlot(resource2);

		availableSlots.add(slot1, 1L);
		availableSlots.add(slot2, 2L);

		availableSlots.tryRemove(slot1.getSlotAllocationId());
		availableSlots.tryRemove(slot2.getSlotAllocationId());

		ArgumentCaptor<AllocatedSlot> argumentCaptor = ArgumentCaptor.forClass(AllocatedSlot.class);
		verify(timerService, times(2)).registerTimeout(argumentCaptor.capture(), eq(10L), eq(TimeUnit.MILLISECONDS));

		List<AllocatedSlot> allocatedSlots = argumentCaptor.getAllValues();
		assertEquals(2, allocatedSlots.size());

		assertTrue(slot1.getSlotAllocationId().equals(allocatedSlots.get(0).getSlotAllocationId()));
		assertTrue(slot2.getSlotAllocationId().equals(allocatedSlots.get(1).getSlotAllocationId()));
	}

	static AllocatedSlot createAllocatedSlot(final ResourceID resourceId) {
		TaskManagerLocation mockTaskManagerLocation = mock(TaskManagerLocation.class);
		when(mockTaskManagerLocation.getResourceID()).thenReturn(resourceId);

		TaskManagerGateway mockTaskManagerGateway = mock(TaskManagerGateway.class);

		return new AllocatedSlot(
			new AllocationID(),
			new JobID(),
			mockTaskManagerLocation,
			0,
			DEFAULT_TESTING_PROFILE,
			mockTaskManagerGateway);
	}
}
