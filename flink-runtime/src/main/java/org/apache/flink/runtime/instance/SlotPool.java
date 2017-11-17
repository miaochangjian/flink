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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotAndLocality;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskexecutor.slot.TimeoutListener;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The slot pool serves slot request issued by Scheduler or ExecutionGraph. It will will attempt to acquire new slots
 * from the ResourceManager when it cannot serve a slot request. If no ResourceManager is currently available,
 * or it gets a decline from the ResourceManager, or a request times out, it fails the slot request. The slot pool also
 * holds all the slots that were offered to it and accepted, and can thus provides registered free slots even if the
 * ResourceManager is down. The slots will only be released when they are useless, e.g. when the job is fully running
 * but we still have some free slots.
 * <p>
 * All the allocation or the slot offering will be identified by self generated AllocationID, we will use it to
 * eliminate ambiguities.
 * 
 * TODO : Make pending requests location preference aware
 * TODO : Make pass location preferences to ResourceManager when sending a slot request
 */
public class SlotPool extends RpcEndpoint implements SlotPoolGateway, TimeoutListener<AllocatedSlot> {

	/** The log for the pool - shared also with the internal classes */
	static final Logger LOG = LoggerFactory.getLogger(SlotPool.class);

	// ------------------------------------------------------------------------

	private final JobID jobId;

	private final ProviderAndOwner providerAndOwner;

	/** All registered TaskManagers, slots will be accepted and used only if the resource is registered */
	private final HashSet<ResourceID> registeredTaskManagers;

	/** The book-keeping of all allocated slots */
	private final AllocatedSlots allocatedSlots;

	/** The book-keeping of all available slots */
	private final AvailableSlots availableSlots;

	/** All pending requests waiting for slots */
	private final DualKeyMap<SlotRequestID, AllocationID, PendingRequest> pendingRequests;

	/** The requests that are waiting for the resource manager to be connected */
	private final HashMap<SlotRequestID, PendingRequest> waitingForResourceManager;

	/** Timeout for request calls to the ResourceManager */
	private final Time resourceManagerRequestsTimeout;

	/** Timeout for allocation round trips (RM -> launch TM -> offer slot) */
	private final Time resourceManagerAllocationTimeout;

	private final Clock clock;

	private final TimerService<AllocatedSlot> timerService;

	/** the fencing token of the job manager */
	private JobMasterId jobMasterId;

	/** The gateway to communicate with resource manager */
	private ResourceManagerGateway resourceManagerGateway;

	private String jobManagerAddress;


	// ------------------------------------------------------------------------

	public SlotPool(
			RpcService rpcService,
			TimerService<AllocatedSlot> timerService,
			JobID jobId,
			Clock clock,
			Time slotAllocationTimeout,
			Time resourceManagerAllocationTimeout,
			Time resourceManagerRequestTimeout,
			Time slotIdleTimeout) {

		super(rpcService);

		this.jobId = checkNotNull(jobId);
		this.clock = checkNotNull(clock);
		this.resourceManagerRequestsTimeout = checkNotNull(resourceManagerRequestTimeout);
		this.resourceManagerAllocationTimeout = checkNotNull(resourceManagerAllocationTimeout);
		this.timerService = checkNotNull(timerService);

		this.registeredTaskManagers = new HashSet<>();
		this.allocatedSlots = new AllocatedSlots();
		this.availableSlots = new AvailableSlots(this.timerService, slotIdleTimeout);
		this.pendingRequests = new DualKeyMap<>(16);
		this.waitingForResourceManager = new HashMap<>(16);

		this.providerAndOwner = new ProviderAndOwner(getSelfGateway(SlotPoolGateway.class), slotAllocationTimeout);

		this.jobMasterId = null;
		this.resourceManagerGateway = null;
		this.jobManagerAddress = null;
	}

	// ------------------------------------------------------------------------
	//  Starting and Stopping
	// ------------------------------------------------------------------------

	@Override
	public void start() {
		throw new UnsupportedOperationException("Should never call start() without leader ID");
	}

	/**
	 * Start the slot pool to accept RPC calls.
	 *
	 * @param jobMasterId The necessary leader id for running the job.
	 * @param newJobManagerAddress for the slot requests which are sent to the resource manager
	 */
	public void start(JobMasterId jobMasterId, String newJobManagerAddress) throws Exception {
		this.jobMasterId = checkNotNull(jobMasterId);
		this.jobManagerAddress = checkNotNull(newJobManagerAddress);

		// TODO - start should not throw an exception
		try {
			super.start();

			this.timerService.start(this);

		} catch (Exception e) {
			throw new RuntimeException("This should never happen", e);
		}
	}

	/**
	 * Suspends this pool, meaning it has lost its authority to accept and distribute slots.
	 */
	@Override
	public void suspend() {
		validateRunsInMainThread();

		// suspend this RPC endpoint
		stop();

		// do not accept any requests
		jobMasterId = null;
		resourceManagerGateway = null;

		// Clear (but not release!) the available slots. The TaskManagers should re-register them
		// at the new leader JobManager/SlotPool
		timerService.unregisterAllTimeouts();

		availableSlots.clear();
		allocatedSlots.clear();
		pendingRequests.clear();
	}

	@Override
	public void postStop() throws Exception {
		timerService.stop();
		super.postStop();
	}

	// ------------------------------------------------------------------------
	//  Getting PoolOwner and PoolProvider
	// ------------------------------------------------------------------------

	/**
	 * Gets the slot owner implementation for this pool.
	 * 
	 * <p>This method does not mutate state and can be called directly (no RPC indirection)
	 * 
	 * @return The slot owner implementation for this pool.
	 */
	public SlotOwner getSlotOwner() {
		return providerAndOwner;
	}

	/**
	 * Gets the slot provider implementation for this pool.
	 *
	 * <p>This method does not mutate state and can be called directly (no RPC indirection)
	 *
	 * @return The slot provider implementation for this pool.
	 */
	public SlotProvider getSlotProvider() {
		return providerAndOwner;
	}

	// ------------------------------------------------------------------------
	//  Resource Manager Connection
	// ------------------------------------------------------------------------

	@Override
	public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
		this.resourceManagerGateway = checkNotNull(resourceManagerGateway);

		// work on all slots waiting for this connection
		for (PendingRequest pendingRequest : waitingForResourceManager.values()) {
			requestSlotFromResourceManager(pendingRequest);
		}

		// all sent off
		waitingForResourceManager.clear();
	}

	@Override
	public void disconnectResourceManager() {
		this.resourceManagerGateway = null;
	}

	// ------------------------------------------------------------------------
	//  Slot Allocation
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<SimpleSlot> allocateSlot(
			SlotRequestID requestId,
			ScheduledUnit task,
			ResourceProfile resources,
			Iterable<TaskManagerLocation> locationPreferences,
			Time timeout) {

		return internalAllocateSlot(requestId, task, resources, locationPreferences);
	}

	@Override
	public void returnAllocatedSlot(Slot slot) {
		internalReturnAllocatedSlot(slot);
	}

	@Override
	public CompletableFuture<Acknowledge> cancelSlotAllocation(SlotRequestID requestId) {
		final PendingRequest pendingRequest = removePendingRequest(requestId);

		if (pendingRequest != null) {
			failPendingRequest(pendingRequest, new CancellationException("Allocation with request id" + requestId + " cancelled."));
		} else {
			final Slot slot = allocatedSlots.get(requestId);

			if (slot != null) {
				LOG.info("Returning allocated slot {} because the corresponding allocation request {} was cancelled.", slot, requestId);
				if (slot.markCancelled()) {
					internalReturnAllocatedSlot(slot);
				}
			} else {
				LOG.debug("There was no slot allocation with {} to be cancelled.", requestId);
			}
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	CompletableFuture<SimpleSlot> internalAllocateSlot(
			SlotRequestID requestId,
			ScheduledUnit task,
			ResourceProfile resources,
			Iterable<TaskManagerLocation> locationPreferences) {

		// (1) do we have a slot available already?
		SlotAndLocality slotFromPool = availableSlots.poll(resources, locationPreferences);
		if (slotFromPool != null) {
			SimpleSlot slot = createSimpleSlot(slotFromPool.slot(), slotFromPool.locality());
			allocatedSlots.add(requestId, slot);
			return CompletableFuture.completedFuture(slot);
		}

		// the request will be completed by a future
		final CompletableFuture<SimpleSlot> future = new CompletableFuture<>();

		// (2) need to request a slot
		if (resourceManagerGateway == null) {
			// no slot available, and no resource manager connection
			stashRequestWaitingForResourceManager(requestId, resources, future);
		} else {
			// we have a resource manager connection, so let's ask it for more resources
			requestSlotFromResourceManager(new PendingRequest(requestId, future, resources));
		}

		return future;
	}

	/**
	 * Checks whether there exists a pending request with the given allocation id and removes it
	 * from the internal data structures.
	 *
	 * @param requestId identifying the pending request
	 * @return pending request if there is one, otherwise null
	 */
	@Nullable
	private PendingRequest removePendingRequest(SlotRequestID requestId) {
		PendingRequest result = waitingForResourceManager.remove(requestId);

		if (result != null) {
			// sanity check
			assert !pendingRequests.containsKeyA(requestId) : "A pending requests should only be part of either " +
				"the pendingRequests or waitingForResourceManager but not both.";

			return result;
		} else {
			return pendingRequests.removeKeyA(requestId);
		}
	}

	private void requestSlotFromResourceManager(
			final PendingRequest pendingRequest) {

		LOG.info("Requesting slot with profile {} from resource manager (request = {}).", pendingRequest.getResourceProfile(), pendingRequest.getSlotRequestId());

		final AllocationID allocationId = new AllocationID();

		pendingRequests.put(pendingRequest.getSlotRequestId(), allocationId, pendingRequest);

		pendingRequest.getFuture().whenComplete(
			(value, throwable) -> {
				if (throwable != null) {
					resourceManagerGateway.cancelSlotRequest(allocationId);
				}
			});

		CompletableFuture<Acknowledge> rmResponse = resourceManagerGateway.requestSlot(
			jobMasterId,
			new SlotRequest(jobId, allocationId, pendingRequest.getResourceProfile(), jobManagerAddress),
			resourceManagerRequestsTimeout);

		CompletableFuture<Void> slotRequestProcessingFuture = rmResponse.thenAcceptAsync(
			(Acknowledge value) -> {
				slotRequestToResourceManagerSuccess(pendingRequest.getSlotRequestId());
			},
			getMainThreadExecutor());

		// on failure, fail the request future
		slotRequestProcessingFuture.whenCompleteAsync(
			(Void v, Throwable failure) -> {
				if (failure != null) {
					slotRequestToResourceManagerFailed(pendingRequest.getSlotRequestId(), failure);
				}
			},
			getMainThreadExecutor());
	}

	private void slotRequestToResourceManagerSuccess(final SlotRequestID requestId) {
		// a request is pending from the ResourceManager to a (future) TaskManager
		// we only add the watcher here in case that request times out
		scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				checkTimeoutSlotAllocation(requestId);
			}
		}, resourceManagerAllocationTimeout);
	}

	private void slotRequestToResourceManagerFailed(SlotRequestID slotRequestID, Throwable failure) {
		PendingRequest request = pendingRequests.removeKeyA(slotRequestID);
		if (request != null) {
			request.getFuture().completeExceptionally(new NoResourceAvailableException(
					"No pooled slot available and request to ResourceManager for new slot failed", failure));
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Unregistered slot request {} failed.", slotRequestID, failure);
			}
		}
	}

	private void checkTimeoutSlotAllocation(SlotRequestID slotRequestID) {
		PendingRequest request = pendingRequests.removeKeyA(slotRequestID);
		if (request != null) {
			failPendingRequest(request, new TimeoutException("Slot allocation request " + slotRequestID + " timed out"));
		}
	}

	private void failPendingRequest(PendingRequest pendingRequest, Exception e) {
		Preconditions.checkNotNull(pendingRequest);
		Preconditions.checkNotNull(e);

		if (!pendingRequest.getFuture().isDone()) {
			pendingRequest.getFuture().completeExceptionally(e);
		}
	}

	private void stashRequestWaitingForResourceManager(
			final SlotRequestID requestId,
			final ResourceProfile resources,
			final CompletableFuture<SimpleSlot> future) {

		LOG.info("Cannot serve slot request, no ResourceManager connected. " +
				"Adding as pending request {}",  requestId);

		waitingForResourceManager.put(requestId, new PendingRequest(requestId, future, resources));

		scheduleRunAsync(new Runnable() {
			@Override
			public void run() {
				checkTimeoutRequestWaitingForResourceManager(requestId);
			}
		}, resourceManagerRequestsTimeout);
	}

	private void checkTimeoutRequestWaitingForResourceManager(SlotRequestID slotRequestId) {
		PendingRequest request = waitingForResourceManager.remove(slotRequestId);
		if (request != null) {
			failPendingRequest(
				request,
				new NoResourceAvailableException("No slot available and no connection to Resource Manager established."));
		}
	}

	// ------------------------------------------------------------------------
	//  Slot releasing & offering
	// ------------------------------------------------------------------------

	/**
	 * Return the slot back to this pool without releasing it. It's mainly called by failed / cancelled tasks, and the
	 * slot can be reused by other pending requests if the resource profile matches.n
	 *
	 * @param slot The slot needs to be returned
	 */
	private void internalReturnAllocatedSlot(Slot slot) {
		checkNotNull(slot);
		checkArgument(!slot.isAlive(), "slot is still alive");
		checkArgument(slot.getOwner() == providerAndOwner, "slot belongs to the wrong pool.");

		// markReleased() is an atomic check-and-set operation, so that the slot is guaranteed
		// to be returned only once
		if (slot.markReleased()) {
			if (allocatedSlots.remove(slot)) {
				// this slot allocation is still valid, use the slot to fulfill another request
				// or make it available again
				final AllocatedSlot taskManagerSlot = slot.getAllocatedSlot();
				final PendingRequest pendingRequest = pollMatchingPendingRequest(taskManagerSlot);
	
				if (pendingRequest != null) {
					LOG.debug("Fulfilling pending request [{}] early with returned slot [{}]",
							pendingRequest.getSlotRequestId(), taskManagerSlot.getSlotAllocationId());

					SimpleSlot newSlot = createSimpleSlot(taskManagerSlot, Locality.UNKNOWN);
					allocatedSlots.add(pendingRequest.getSlotRequestId(), newSlot);
					pendingRequest.getFuture().complete(newSlot);
				}
				else {
					LOG.debug("Adding returned slot [{}] to available slots", taskManagerSlot.getSlotAllocationId());
					availableSlots.add(taskManagerSlot, clock.relativeTimeMillis());
				}
			}
			else {
				LOG.debug("Returned slot's allocation has been failed. Dropping slot.");
			}
		}
	}

	private PendingRequest pollMatchingPendingRequest(final AllocatedSlot slot) {
		final ResourceProfile slotResources = slot.getResourceProfile();

		// try the requests sent to the resource manager first
		for (PendingRequest request : pendingRequests.values()) {
			if (slotResources.isMatching(request.getResourceProfile())) {
				pendingRequests.removeKeyA(request.getSlotRequestId());
				return request;
			}
		}

		// try the requests waiting for a resource manager connection next
		for (PendingRequest request : waitingForResourceManager.values()) {
			if (slotResources.isMatching(request.getResourceProfile())) {
				waitingForResourceManager.remove(request.getSlotRequestId());
				return request;
			}
		}

		// no request pending, or no request matches
		return null;
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(Collection<Tuple2<AllocatedSlot, SlotOffer>> offers) {
		validateRunsInMainThread();

		List<CompletableFuture<Optional<SlotOffer>>> acceptedSlotOffers = offers.stream().map(
			offer -> {
				CompletableFuture<Optional<SlotOffer>> acceptedSlotOffer = offerSlot(offer.f0).thenApply(
					(acceptedSlot) -> {
						if (acceptedSlot) {
							return Optional.of(offer.f1);
						} else {
							return Optional.empty();
						}
					});

				return acceptedSlotOffer;
			}
		).collect(Collectors.toList());

		CompletableFuture<Collection<Optional<SlotOffer>>> optionalSlotOffers = FutureUtils.combineAll(acceptedSlotOffers);

		CompletableFuture<Collection<SlotOffer>> resultingSlotOffers = optionalSlotOffers.thenApply(
			collection -> {
				Collection<SlotOffer> slotOffers = collection
					.stream()
					.flatMap(
						opt -> opt.map(Stream::of).orElseGet(Stream::empty))
					.collect(Collectors.toList());

				return slotOffers;
			});

		return resultingSlotOffers;
	}
	
	/**
	 * Slot offering by TaskManager with AllocationID. The AllocationID is originally generated by this pool and
	 * transfer through the ResourceManager to TaskManager. We use it to distinguish the different allocation
	 * we issued. Slot offering may be rejected if we find something mismatching or there is actually no pending
	 * request waiting for this slot (maybe fulfilled by some other returned slot).
	 *
	 * @param slot The offered slot
	 * @return True if we accept the offering
	 */
	@Override
	public CompletableFuture<Boolean> offerSlot(final AllocatedSlot slot) {
		validateRunsInMainThread();

		// check if this TaskManager is valid
		final ResourceID resourceID = slot.getTaskManagerId();
		final AllocationID allocationID = slot.getSlotAllocationId();

		if (!registeredTaskManagers.contains(resourceID)) {
			LOG.debug("Received outdated slot offering [{}] from unregistered TaskManager: {}",
					slot.getSlotAllocationId(), slot);
			return CompletableFuture.completedFuture(false);
		}

		// check whether we have already using this slot
		if (allocatedSlots.contains(allocationID) || availableSlots.contains(allocationID)) {
			LOG.debug("Received repeated offer for slot [{}]. Ignoring.", allocationID);

			// return true here so that the sender will get a positive acknowledgement to the retry
			// and mark the offering as a success
			return CompletableFuture.completedFuture(true);
		}

		// check whether we have request waiting for this slot
		PendingRequest pendingRequest = pendingRequests.removeKeyB(allocationID);
		if (pendingRequest != null) {
			// we were waiting for this!
			SimpleSlot resultSlot = createSimpleSlot(slot, Locality.UNKNOWN);
			pendingRequest.getFuture().complete(resultSlot);
			allocatedSlots.add(pendingRequest.getSlotRequestId(), resultSlot);
		}
		else {
			// we were actually not waiting for this:
			//   - could be that this request had been fulfilled
			//   - we are receiving the slots from TaskManagers after becoming leaders
			availableSlots.add(slot, clock.relativeTimeMillis());
		}

		// we accepted the request in any case. slot will be released after it idled for
		// too long and timed out
		return CompletableFuture.completedFuture(true);
	}

	
	// TODO - periodic (every minute or so) catch slots that were lost (check all slots, if they have any task active)

	// TODO - release slots that were not used to the resource manager

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Fail the specified allocation and release the corresponding slot if we have one.
	 * This may triggered by JobManager when some slot allocation failed with timeout.
	 * Or this could be triggered by TaskManager, when it finds out something went wrong with the slot,
	 * and decided to take it back.
	 *
	 * @param allocationID Represents the allocation which should be failed
	 * @param cause        The cause of the failure
	 */
	@Override
	public void failAllocation(final AllocationID allocationID, final Exception cause) {
		final PendingRequest pendingRequest = pendingRequests.removeKeyB(allocationID);
		if (pendingRequest != null) {
			// request was still pending
			failPendingRequest(pendingRequest, cause);
		}
		else if (availableSlots.tryRemove(allocationID)) {
			LOG.debug("Failed available slot [{}] with ", allocationID, cause);
		}
		else {
			Slot slot = allocatedSlots.remove(allocationID);
			if (slot != null) {
				// release the slot.
				// since it is not in 'allocatedSlots' any more, it will be dropped o return'
				slot.releaseSlot();
			}
			else {
				LOG.debug("Outdated request to fail slot [{}] with ", allocationID, cause);
			}
		}
		// TODO: add some unit tests when the previous two are ready, the allocation may failed at any phase
	}

	// ------------------------------------------------------------------------
	//  Resource
	// ------------------------------------------------------------------------

	/**
	 * Register TaskManager to this pool, only those slots come from registered TaskManager will be considered valid.
	 * Also it provides a way for us to keep "dead" or "abnormal" TaskManagers out of this pool.
	 *
	 * @param resourceID The id of the TaskManager
	 * @return Future acknowledge if th operation was successful
	 */
	@Override
	public CompletableFuture<Acknowledge> registerTaskManager(final ResourceID resourceID) {
		registeredTaskManagers.add(resourceID);

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Unregister TaskManager from this pool, all the related slots will be released and tasks be canceled. Called
	 * when we find some TaskManager becomes "dead" or "abnormal", and we decide to not using slots from it anymore.
	 *
	 * @param resourceID The id of the TaskManager
	 */
	@Override
	public CompletableFuture<Acknowledge> releaseTaskManager(final ResourceID resourceID) {
		if (registeredTaskManagers.remove(resourceID)) {
			availableSlots.removeAllForTaskManager(resourceID);

			final Set<Slot> allocatedSlotsForResource = allocatedSlots.removeSlotsForTaskManager(resourceID);
			for (Slot slot : allocatedSlotsForResource) {
				slot.releaseSlot();
			}
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void notifyTimeout(AllocatedSlot key, UUID ticket) {
		runAsync(() -> {
			if (availableSlots.tryRemove(key.getSlotAllocationId())) {
				LOG.info("Notify unused slot {} time out for task manager.", key.getSlotAllocationId());

				// What shall we do if notify failed?
				// If asking timeout, the heartbeat between TM and RM will fix the slot leaking.
				// If some unexpected exceptions happened, maybe retry is a better choice.
				// Now, just ignore exception checking here.
				key.getTaskManagerGateway().notifySlotUnused(key.getSlotAllocationId());
			}
		});
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private SimpleSlot createSimpleSlot(AllocatedSlot slot, Locality locality) {
		SimpleSlot result = new SimpleSlot(slot, providerAndOwner, slot.getSlotNumber());
		if (locality != null) {
			result.setLocality(locality);
		}
		return result;
	}

	// ------------------------------------------------------------------------
	//  Methods for tests
	// ------------------------------------------------------------------------

	@VisibleForTesting
	AllocatedSlots getAllocatedSlots() {
		return allocatedSlots;
	}

	@VisibleForTesting
	AvailableSlots getAvailableSlots() {
		return availableSlots;
	}

	@VisibleForTesting
	DualKeyMap<SlotRequestID, AllocationID, PendingRequest> getPendingRequests() {
		return pendingRequests;
	}

	@VisibleForTesting
	Map<SlotRequestID, PendingRequest> getWaitingForResourceManager() {
		return waitingForResourceManager;
	}

	// ------------------------------------------------------------------------
	//  Helper classes
	// ------------------------------------------------------------------------

	/**
	 * Organize allocated slots from different points of view.
	 */
	static class AllocatedSlots {

		/** All allocated slots organized by TaskManager's id */
		private final Map<ResourceID, Set<Slot>> allocatedSlotsByTaskManager;

		/** All allocated slots organized by AllocationID */
		private final DualKeyMap<AllocationID, SlotRequestID, Slot> allocatedSlotsById;

		AllocatedSlots() {
			this.allocatedSlotsByTaskManager = new HashMap<>(16);
			this.allocatedSlotsById = new DualKeyMap<>(16);
		}

		/**
		 * Adds a new slot to this collection.
		 *
		 * @param slot The allocated slot
		 */
		void add(SlotRequestID slotRequestId, Slot slot) {
			allocatedSlotsById.put(slot.getAllocatedSlot().getSlotAllocationId(), slotRequestId, slot);

			final ResourceID resourceID = slot.getTaskManagerID();
			Set<Slot> slotsForTaskManager = allocatedSlotsByTaskManager.get(resourceID);
			if (slotsForTaskManager == null) {
				slotsForTaskManager = new HashSet<>();
				allocatedSlotsByTaskManager.put(resourceID, slotsForTaskManager);
			}
			slotsForTaskManager.add(slot);
		}

		/**
		 * Get allocated slot with allocation id
		 *
		 * @param allocationID The allocation id
		 * @return The allocated slot, null if we can't find a match
		 */
		Slot get(final AllocationID allocationID) {
			return allocatedSlotsById.getKeyA(allocationID);
		}

		Slot get(final SlotRequestID slotRequestId) {
			return allocatedSlotsById.getKeyB(slotRequestId);
		}

		/**
		 * Check whether we have allocated this slot
		 *
		 * @param slotAllocationId The allocation id of the slot to check
		 * @return True if we contains this slot
		 */
		boolean contains(AllocationID slotAllocationId) {
			return allocatedSlotsById.containsKeyA(slotAllocationId);
		}

		/**
		 * Remove an allocation with slot.
		 *
		 * @param slot The slot needs to be removed
		 */
		boolean remove(final Slot slot) {
			return remove(slot.getAllocatedSlot().getSlotAllocationId()) != null;
		}

		/**
		 * Remove an allocation with slot.
		 *
		 * @param slotId The ID of the slot to be removed
		 */
		Slot remove(final AllocationID slotId) {
			Slot slot = allocatedSlotsById.removeKeyA(slotId);
			if (slot != null) {
				final ResourceID taskManagerId = slot.getTaskManagerID();
				Set<Slot> slotsForTM = allocatedSlotsByTaskManager.get(taskManagerId);
				slotsForTM.remove(slot);
				if (slotsForTM.isEmpty()) {
					allocatedSlotsByTaskManager.remove(taskManagerId);
				}
				return slot;
			}
			else {
				return null;
			}
		}

		/**
		 * Get all allocated slot from same TaskManager.
		 *
		 * @param resourceID The id of the TaskManager
		 * @return Set of slots which are allocated from the same TaskManager
		 */
		Set<Slot> removeSlotsForTaskManager(final ResourceID resourceID) {
			Set<Slot> slotsForTaskManager = allocatedSlotsByTaskManager.remove(resourceID);
			if (slotsForTaskManager != null) {
				for (Slot slot : slotsForTaskManager) {
					allocatedSlotsById.removeKeyA(slot.getAllocatedSlot().getSlotAllocationId());
				}
				return slotsForTaskManager;
			}
			else {
				return Collections.emptySet();
			}
		}

		void clear() {
			allocatedSlotsById.clear();
			allocatedSlotsByTaskManager.clear();
		}

		@VisibleForTesting
		boolean containResource(final ResourceID resourceID) {
			return allocatedSlotsByTaskManager.containsKey(resourceID);
		}

		@VisibleForTesting
		int size() {
			return allocatedSlotsById.size();
		}

		@VisibleForTesting
		Set<Slot> getSlotsForTaskManager(ResourceID resourceId) {
			if (allocatedSlotsByTaskManager.containsKey(resourceId)) {
				return allocatedSlotsByTaskManager.get(resourceId);
			} else {
				return Collections.emptySet();
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Organize all available slots from different points of view.
	 */
	static class AvailableSlots {

		/** All available slots organized by TaskManager */
		private final HashMap<ResourceID, Set<AllocatedSlot>> availableSlotsByTaskManager;

		/** All available slots organized by host */
		private final HashMap<String, Set<AllocatedSlot>> availableSlotsByHost;

		/** The available slots, with the time when they were inserted */
		private final HashMap<AllocationID, SlotAndTimestamp> availableSlots;

		private final TimerService<AllocatedSlot> timerService;

		private final long idleSlotTimeoutMs;

		AvailableSlots() {
			this(null, null);
		}

		AvailableSlots(@Nullable TimerService<AllocatedSlot> timerService, @Nullable Time idleSlotTimeout) {
			this.availableSlotsByTaskManager = new HashMap<>();
			this.availableSlotsByHost = new HashMap<>();
			this.availableSlots = new HashMap<>();
			this.timerService = timerService;
			this.idleSlotTimeoutMs = idleSlotTimeout != null ? idleSlotTimeout.toMilliseconds() : -1;
		}

		/**
		 * Adds an available slot.
		 *
		 * @param slot The slot to add
		 */
		void add(final AllocatedSlot slot, final long timestamp) {
			checkNotNull(slot);

			SlotAndTimestamp previous = availableSlots.put(
					slot.getSlotAllocationId(), new SlotAndTimestamp(slot, timestamp));

			if (previous == null) {
				final ResourceID resourceID = slot.getTaskManagerLocation().getResourceID();
				final String host = slot.getTaskManagerLocation().getFQDNHostname();

				Set<AllocatedSlot> slotsForTaskManager = availableSlotsByTaskManager.get(resourceID);
				if (slotsForTaskManager == null) {
					slotsForTaskManager = new HashSet<>();
					availableSlotsByTaskManager.put(resourceID, slotsForTaskManager);
				}
				slotsForTaskManager.add(slot);

				Set<AllocatedSlot> slotsForHost = availableSlotsByHost.get(host);
				if (slotsForHost == null) {
					slotsForHost = new HashSet<>();
					availableSlotsByHost.put(host, slotsForHost);
				}
				slotsForHost.add(slot);

				if (timerService != null) {
					timerService.registerTimeout(slot, idleSlotTimeoutMs, TimeUnit.MILLISECONDS);
				}
			}
			else {
				throw new IllegalStateException("slot already contained");
			}
		}

		/**
		 * Check whether we have this slot.
		 */
		boolean contains(AllocationID slotId) {
			return availableSlots.containsKey(slotId);
		}

		/**
		 * Poll a slot which matches the required resource profile. The polling tries to satisfy the
		 * location preferences, by TaskManager and by host.
		 *
		 * @param resourceProfile      The required resource profile.
		 * @param locationPreferences  The location preferences, in order to be checked.
		 * 
		 * @return Slot which matches the resource profile, null if we can't find a match
		 */
		SlotAndLocality poll(ResourceProfile resourceProfile, Iterable<TaskManagerLocation> locationPreferences) {
			// fast path if no slots are available
			if (availableSlots.isEmpty()) {
				return null;
			}

			boolean hadLocationPreference = false;

			if (locationPreferences != null) {

				// first search by TaskManager
				for (TaskManagerLocation location : locationPreferences) {
					hadLocationPreference = true;

					final Set<AllocatedSlot> onTaskManager = availableSlotsByTaskManager.get(location.getResourceID());
					if (onTaskManager != null) {
						for (AllocatedSlot candidate : onTaskManager) {
							if (candidate.getResourceProfile().isMatching(resourceProfile)) {
								remove(candidate.getSlotAllocationId());
								return new SlotAndLocality(candidate, Locality.LOCAL);
							}
						}
					}
				}

				// now, search by host
				for (TaskManagerLocation location : locationPreferences) {
					final Set<AllocatedSlot> onHost = availableSlotsByHost.get(location.getFQDNHostname());
					if (onHost != null) {
						for (AllocatedSlot candidate : onHost) {
							if (candidate.getResourceProfile().isMatching(resourceProfile)) {
								remove(candidate.getSlotAllocationId());
								return new SlotAndLocality(candidate, Locality.HOST_LOCAL);
							}
						}
					}
				}
			}

			// take any slot
			for (SlotAndTimestamp candidate : availableSlots.values()) {
				final AllocatedSlot slot = candidate.slot();

				if (slot.getResourceProfile().isMatching(resourceProfile)) {
					remove(slot.getSlotAllocationId());
					return new SlotAndLocality(
							slot, hadLocationPreference ? Locality.NON_LOCAL : Locality.UNCONSTRAINED);
				}
			}

			// nothing available that matches
			return null;
		}

		/**
		 * Remove all available slots come from specified TaskManager.
		 *
		 * @param taskManager The id of the TaskManager
		 */
		void removeAllForTaskManager(final ResourceID taskManager) {
			// remove from the by-TaskManager view
			final Set<AllocatedSlot> slotsForTm = availableSlotsByTaskManager.remove(taskManager);

			if (slotsForTm != null && slotsForTm.size() > 0) {
				final String host = slotsForTm.iterator().next().getTaskManagerLocation().getFQDNHostname();
				final Set<AllocatedSlot> slotsForHost = availableSlotsByHost.get(host);

				// remove from the base set and the by-host view
				for (AllocatedSlot slot : slotsForTm) {
					availableSlots.remove(slot.getSlotAllocationId());
					slotsForHost.remove(slot);

					if (timerService != null) {
						timerService.unregisterTimeout(slot);
					}
				}

				if (slotsForHost.isEmpty()) {
					availableSlotsByHost.remove(host);
				}
			}
		}

		boolean tryRemove(AllocationID slotId) {
			final SlotAndTimestamp sat = availableSlots.remove(slotId);
			if (sat != null) {
				final AllocatedSlot slot = sat.slot();
				final ResourceID resourceID = slot.getTaskManagerLocation().getResourceID();
				final String host = slot.getTaskManagerLocation().getFQDNHostname();

				final Set<AllocatedSlot> slotsForTm = availableSlotsByTaskManager.get(resourceID);
				final Set<AllocatedSlot> slotsForHost = availableSlotsByHost.get(host);

				slotsForTm.remove(slot);
				slotsForHost.remove(slot);

				if (slotsForTm.isEmpty()) {
					availableSlotsByTaskManager.remove(resourceID);
				}
				if (slotsForHost.isEmpty()) {
					availableSlotsByHost.remove(host);
				}

				if (timerService != null) {
					timerService.unregisterTimeout(slot);
				}

				return true;
			}
			else {
				return false;
			}
		}

		private void remove(AllocationID slotId) throws IllegalStateException {
			if (!tryRemove(slotId)) {
				throw new IllegalStateException("slot not contained");
			}
		}

		@VisibleForTesting
		boolean containsTaskManager(ResourceID resourceID) {
			return availableSlotsByTaskManager.containsKey(resourceID);
		}

		@VisibleForTesting
		int size() {
			return availableSlots.size();
		}

		@VisibleForTesting
		void clear() {
			availableSlots.clear();
			availableSlotsByTaskManager.clear();
			availableSlotsByHost.clear();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * An implementation of the {@link SlotOwner} and {@link SlotProvider} interfaces
	 * that delegates methods as RPC calls to the SlotPool's RPC gateway.
	 */
	private static class ProviderAndOwner implements SlotOwner, SlotProvider {

		private final SlotPoolGateway gateway;

		private final Time timeout;

		ProviderAndOwner(SlotPoolGateway gateway, Time timeout) {
			this.gateway = gateway;
			this.timeout = timeout;
		}

		@Override
		public boolean returnAllocatedSlot(Slot slot) {
			gateway.returnAllocatedSlot(slot);
			return true;
		}

		@Override
		public CompletableFuture<SimpleSlot> allocateSlot(
				ScheduledUnit task,
				boolean allowQueued,
				Collection<TaskManagerLocation> preferredLocations) {

			final SlotRequestID requestId = new SlotRequestID();
			CompletableFuture<SimpleSlot> slotFuture = gateway.allocateSlot(requestId, task, ResourceProfile.UNKNOWN, preferredLocations, timeout);
			slotFuture.whenComplete(
				(SimpleSlot slot, Throwable failure) -> {
					if (failure != null) {
						gateway.cancelSlotAllocation(requestId);
					}
			});
			return slotFuture;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A pending request for a slot
	 */
	private static class PendingRequest {

		private final SlotRequestID slotRequestId;

		private final CompletableFuture<SimpleSlot> future;

		private final ResourceProfile resourceProfile;

		PendingRequest(
				SlotRequestID slotRequestId,
				CompletableFuture<SimpleSlot> future,
				ResourceProfile resourceProfile) {
			this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
			this.future = Preconditions.checkNotNull(future);
			this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
		}

		public SlotRequestID getSlotRequestId() {
			return slotRequestId;
		}

		public CompletableFuture<SimpleSlot> getFuture() {
			return future;
		}

		public ResourceProfile getResourceProfile() {
			return resourceProfile;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A slot, together with the timestamp when it was added
	 */
	private static class SlotAndTimestamp {

		private final AllocatedSlot slot;

		private final long timestamp;

		SlotAndTimestamp(AllocatedSlot slot, long timestamp) {
			this.slot = slot;
			this.timestamp = timestamp;
		}

		public AllocatedSlot slot() {
			return slot;
		}

		public long timestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return slot + " @ " + timestamp;
		}
	}
}
