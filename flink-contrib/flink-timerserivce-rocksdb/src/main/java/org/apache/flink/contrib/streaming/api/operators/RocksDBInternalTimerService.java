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

package org.apache.flink.contrib.streaming.api.operators;


import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.StringAppendOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link InternalTimerService} that stores timers in RocksDB.
 */
public class RocksDBInternalTimerService<K, N> extends InternalTimerService<K, N> {
	
	private static Logger LOG = LoggerFactory.getLogger(RocksDBInternalTimerService.class);
	
	/** The data base where stores all timers */
	private final RocksDB db;
	
	/** The path where the rocksdb locates */
	private final Path dbPath;

	/**
	 * The in-memory heaps backed by rocksdb to retrieve the next timer to trigger. Each
	 * partition's leader is stored in the heap. When the timers in a partition is changed, we
	 * will change the partition's leader and update the heap accordingly.
	 */
	private final int numPartitions;
	private final PersistentTimerHeap eventTimeHeap;
	private final PersistentTimerHeap processingTimeHeap;
	
	private static int MAX_PARTITIONS = (1 << 16);

	public RocksDBInternalTimerService(
			int totalKeyGroups,
			KeyGroupRange keyGroupRange,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService,
			Path dbPath) {

		super(totalKeyGroups, keyGroupRange, keyContext, processingTimeService);
		
		this.dbPath = dbPath;
		
		try {
			FileSystem fileSystem = this.dbPath.getFileSystem();
			if (fileSystem.exists(this.dbPath)) {
				fileSystem.delete(this.dbPath, true);
			}
			
			fileSystem.mkdirs(dbPath);
		} catch (IOException e) {
			throw new RuntimeException("Error while creating directory for rocksdb timer service.", e);
		}

		ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
				.setMergeOperator(new StringAppendOperator())
				.setCompactionStyle(CompactionStyle.UNIVERSAL);
		ColumnFamilyDescriptor defaultColumnDescriptor = new ColumnFamilyDescriptor("default".getBytes(), columnFamilyOptions);

		DBOptions dbOptions = new DBOptions()
				.setCreateIfMissing(true)
				.setUseFsync(false)
				.setDisableDataSync(true)
				.setMaxOpenFiles(-1);

		List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);

		try {
			this.db = RocksDB.open(dbOptions, dbPath.getPath(), Collections.singletonList(defaultColumnDescriptor), columnFamilyHandles);
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while creating the RocksDB instance.", e);
		}

		this.numPartitions = Math.min(keyGroupRange.getNumberOfKeyGroups(), MAX_PARTITIONS);

		ColumnFamilyHandle eventTimeColumnFamilyHandle;
		ColumnFamilyHandle processingTimeColumnFamilyHandle;
		try {
			ColumnFamilyDescriptor eventTimeColumnFamilyDescriptor = new ColumnFamilyDescriptor("eventTime".getBytes(), columnFamilyOptions);
			ColumnFamilyDescriptor processingTimeColumnFamilyDescriptor = new ColumnFamilyDescriptor("processingTime".getBytes(), columnFamilyOptions);
			eventTimeColumnFamilyHandle = db.createColumnFamily(eventTimeColumnFamilyDescriptor);
			processingTimeColumnFamilyHandle = db.createColumnFamily(processingTimeColumnFamilyDescriptor);
		} catch (RocksDBException e) {
			throw new RuntimeException("Error while creating the column families.", e);
		}

		this.processingTimeHeap = new PersistentTimerHeap(numPartitions, processingTimeColumnFamilyHandle);
		this.eventTimeHeap = new PersistentTimerHeap(numPartitions, eventTimeColumnFamilyHandle);
	}

	// ------------------------------------------------------------------------
	//  InternalTimerService Implementation
	// ------------------------------------------------------------------------
	
	@Override
	public void start() {
		// rebuild the heaps
		eventTimeHeap.initialize();
		processingTimeHeap.initialize();
		
		// register the processing timer with the minimum timestamp
		Tuple4<Integer, Long, K, N> headProcessingTimer = processingTimeHeap.top();
		if (headProcessingTimer != null) {
			nextTimer = processingTimeService.registerTimer(headProcessingTimer.f1, this);
		}
	}

	@Override
	public void close() {
		if (db != null) {
			db.close();
		}
		
		if (dbPath != null) {
			try {
				FileSystem fileSystem = dbPath.getFileSystem();
				if (fileSystem.exists(dbPath)) {
					fileSystem.delete(dbPath, true);
				}
			} catch (IOException e) {
				throw new RuntimeException("Error while cleaning directory for rocksdb timer service.", e);
			}
		}
	}

	@Override
	public void onEventTime(long timestamp) throws Exception {
		List<Tuple4<Integer, Long, K, N>> timers = eventTimeHeap.peek(timestamp);
		for (Tuple4<Integer, Long, K, N> timer : timers) {
			keyContext.setCurrentKey(timer.f2);
			triggerTarget.onEventTime(new InternalTimer<>(timer.f1, timer.f2, timer.f3));
		}
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		nextTimer = null;

		List<Tuple4<Integer, Long, K, N>> timers = processingTimeHeap.peek(timestamp);
		for (Tuple4<Integer, Long, K, N> timer : timers) {
			keyContext.setCurrentKey(timer.f2);
			triggerTarget.onProcessingTime(new InternalTimer<>(timer.f1, timer.f2, timer.f3));
		}

		if (nextTimer == null) {
			Tuple4<Integer, Long, K, N> headTimer = processingTimeHeap.top();
			if (headTimer != null) {
				nextTimer = processingTimeService.registerTimer(headTimer.f1, this);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		boolean isNewHead = processingTimeHeap.add((K)keyContext.getCurrentKey(), namespace, time);

		if (isNewHead) {
			if (nextTimer != null) {
				nextTimer.cancel(false);
			}

			Tuple4<Integer, Long, K, N> newHeadTimer = processingTimeHeap.top();
			if (newHeadTimer == null || newHeadTimer.f1 != time) {
				throw new IllegalStateException();
			}

			nextTimer = processingTimeService.registerTimer(newHeadTimer.f1, this);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		boolean isCurrentHead = processingTimeHeap.remove((K)keyContext.getCurrentKey(), namespace, time);

		if (isCurrentHead) {
			if (nextTimer != null) {
				nextTimer.cancel(false);
			}

			Tuple4<Integer, Long, K, N> newHeadTimer = processingTimeHeap.top();
			if (newHeadTimer != null) {
				if (newHeadTimer.f1 < time) {
					throw new IllegalStateException();
				}

				nextTimer = processingTimeService.registerTimer(newHeadTimer.f1, this);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		eventTimeHeap.add((K)keyContext.getCurrentKey(), namespace, time);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		eventTimeHeap.remove((K)keyContext.getCurrentKey(), namespace, time);
	}

	@Override
	public Set<InternalTimer<K, N>> getEventTimeTimersForKeyGroup(int keyGroup) {
		return eventTimeHeap.getTimers(keyGroup);
	}

	@Override
	public Set<InternalTimer<K, N>> getProcessingTimeTimersForKeyGroup(int keyGroup) {
		return processingTimeHeap.getTimers(keyGroup);
	}

	@Override
	public void restoreEventTimeTimersForKeyGroup(int keyGroup, Iterable<InternalTimer<K, N>> internalTimers) {
		eventTimeHeap.restoreTimers(keyGroup, internalTimers);
	}

	@Override
	public void restoreProcessingTimeTimersForKeyGroup(int keyGroup, Iterable<InternalTimer<K, N>> internalTimers) {
		processingTimeHeap.restoreTimers(keyGroup, internalTimers);
	}

	@Override
	public int numProcessingTimeTimers() {
		return processingTimeHeap.numTimers(null);
	}

	@Override
	public int numEventTimeTimers() {
		return eventTimeHeap.numTimers(null);
	}

	@Override
	public int numProcessingTimeTimers(N namespace) {
		return processingTimeHeap.numTimers(namespace);
	}

	@Override
	public int numEventTimeTimers(N namespace) {
		return eventTimeHeap.numTimers(namespace);
	}
	
	// ------------------------------------------------------------------------
	//  Partitioning Methods
	// ------------------------------------------------------------------------

	/**
	 * Assigns the given key group to a partition.
	 */
	private static int getPartitionForKeyGroup(KeyGroupRange keyGroupRange, int keyGroup, int numPartitions) {
		Preconditions.checkArgument(keyGroupRange != null, "The range must not be null");
		Preconditions.checkArgument(numPartitions > 0, "Partition count must not be smaller than zero.");

		Preconditions.checkArgument(keyGroup >= keyGroupRange.getStartKeyGroup() && keyGroup <= keyGroupRange.getEndKeyGroup(), "Key group must be in the range");

		long numKeyGroupsPerPartition = (keyGroupRange.getEndKeyGroup() - keyGroupRange.getStartKeyGroup() + 1L) / numPartitions;
		long numFatPartitions = (keyGroupRange.getEndKeyGroup() - keyGroupRange.getStartKeyGroup() + 1L) - numKeyGroupsPerPartition * numPartitions;

		keyGroup -= keyGroupRange.getStartKeyGroup();

		if (keyGroup >= (numKeyGroupsPerPartition + 1L) * numFatPartitions) {
			return (int)((keyGroup - (numKeyGroupsPerPartition + 1L) * numFatPartitions) / numKeyGroupsPerPartition + numFatPartitions);
		} else {
			return (int)(keyGroup / (numKeyGroupsPerPartition + 1L));
		}
	}

	/**
	 * Compute the range of the given partition
	 */
	private static KeyGroupRange getRangeForPartition(KeyGroupRange keyGroupRange, int partitionIndex, int numPartitions) {
		Preconditions.checkArgument(keyGroupRange != null, "The range must not be null");
		Preconditions.checkArgument(partitionIndex >= 0, "Partition index must be not smaller than zero.");
		Preconditions.checkArgument(numPartitions > 0, "Partition count must be greater than zero.");

		long numKeysPerPartition = (keyGroupRange.getEndKeyGroup() - keyGroupRange.getStartKeyGroup() + 1L) / numPartitions;
		long numFatPartitions = (keyGroupRange.getEndKeyGroup() - keyGroupRange.getStartKeyGroup() + 1L) - numKeysPerPartition * numPartitions;

		if (partitionIndex >= numFatPartitions) {
			int startKeyGroup = keyGroupRange.getStartKeyGroup() + (int)(numFatPartitions * (numKeysPerPartition + 1L) + (partitionIndex - numFatPartitions) * numKeysPerPartition);
			int endKeyGroup = (int)(startKeyGroup + numKeysPerPartition - 1L);

			return (startKeyGroup > endKeyGroup ? null : new KeyGroupRange(startKeyGroup, endKeyGroup));
		} else {
			int startKeyGroup = keyGroupRange.getStartKeyGroup() + (int)(partitionIndex * (numKeysPerPartition + 1L));
			int endKeyGroup = (int)(startKeyGroup + numKeysPerPartition);

			return new KeyGroupRange(startKeyGroup, endKeyGroup);
		}
	}

	// ------------------------------------------------------------------------
	//  Serialization Methods
	// ------------------------------------------------------------------------

	private byte[] serializeKeyGroup(int keyGroup) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

		try {
			IntSerializer.INSTANCE.serialize(keyGroup, outputView);
		} catch (IOException e) {
			throw new RuntimeException("Error while deserializing the key group.", e);
		}

		return outputStream.toByteArray();
	}

	private byte[] serializeRawTimer(Tuple4<Integer, Long, K, N> rawTimer) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

		try {
			IntSerializer.INSTANCE.serialize(rawTimer.f0, outputView);
			LongSerializer.INSTANCE.serialize(rawTimer.f1, outputView);
			keySerializer.serialize(rawTimer.f2, outputView);
			namespaceSerializer.serialize(rawTimer.f3, outputView);
		} catch (IOException e) {
			throw new RuntimeException("Error while serializing the raw timer.", e);
		}

		return outputStream.toByteArray();
	}

	private Tuple4<Integer, Long, K, N> deserializeRawTimer(byte[] bytes) {
		ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

		try {
			int keyGroup = IntSerializer.INSTANCE.deserialize(inputView);
			long timestamp = LongSerializer.INSTANCE.deserialize(inputView);
			K key = keySerializer.deserialize(inputView);
			N namespace = namespaceSerializer.deserialize(inputView);

			return new Tuple4<>(keyGroup, timestamp, key, namespace);
		} catch (IOException e) {
			throw new RuntimeException("Error while deserializing the raw timer.", e);
		}
	}

	/**
	 * A timer store backed by RocksDB. 
	 * 
	 * The timers are stored in RocksDB in the order of key groups. To allow 
	 * efficient access, the timers are partitioned and the leader of each 
	 * partition is stored in an in-memory heap. The top of the heap is 
	 * exactly the first timer to trigger. The heap is updated whenever the 
	 * partition's leader is updated.
	 */
	private class PersistentTimerHeap {

		private final ColumnFamilyHandle handle;

		/** Leader timers in the partitions */
		private final Tuple4<Integer, Long, K, N>[] partitionHeadTimers;
		
		/** The order of the partition's leader in the heap */
		private final int[] orders;
		
		/** The partitions in the order of their leaders in the heap */
		private final int[] indices;
		
		@SuppressWarnings("unchecked")
		PersistentTimerHeap(int capacity, ColumnFamilyHandle handle) {
			this.handle = handle;

			this.partitionHeadTimers = (Tuple4<Integer, Long, K, N>[])new Tuple4[capacity];
			this.indices = new int[capacity];
			this.orders = new int[capacity];
			
			for (int i = 0; i < capacity; i++) {
				orders[i] = i;
				indices[i] = i;
			}
		}

		private int getParentOrder(int order) {
			return (order - 1) / 2;
		}

		private int getLeftChildOrder(int order) {
			return order * 2 + 1;
		}

		private int getRightChildOrder(int order) {
			return order * 2 + 2;
		}

		private long getTimestamp(Tuple4<Integer, Long, K, N> rawTimer) {
			return rawTimer == null ? Long.MAX_VALUE : rawTimer.f1;
		}

		/** Rebuild the heap with the given DB. */
		void initialize() {
			int currentPartition = -1;
			Tuple4<Integer, Long, K, N> currentPartitionLeaderTimer = null;

			RocksIterator iterator = db.newIterator(handle);
			iterator.seekToFirst();

			while (iterator.isValid()) {
				Tuple4<Integer, Long, K, N> rawTimer = deserializeRawTimer(iterator.key());

				int partition = getPartitionForKeyGroup(keyGroupRange, rawTimer.f0, numPartitions);
				if (partition == currentPartition) {
					if (currentPartitionLeaderTimer == null || rawTimer.f1 < currentPartitionLeaderTimer.f1) {
						currentPartitionLeaderTimer = rawTimer;
					}
				} else {
					if (currentPartitionLeaderTimer != null) {
						updatePartitionLeader(currentPartition, currentPartitionLeaderTimer);
					}

					currentPartition = partition;
					currentPartitionLeaderTimer = rawTimer;
				}

				int nextKeyGroup = rawTimer.f0 + 1;
				byte[] nextKeyGroupBytes = serializeKeyGroup(nextKeyGroup);
				iterator.seek(nextKeyGroupBytes);
			}

			iterator.close();

			if (currentPartitionLeaderTimer != null) {
				updatePartitionLeader(currentPartition, currentPartitionLeaderTimer);
			}
		}

		/**
		 * The first timer in the store.
		 */
		Tuple4<Integer, Long, K, N> top() {
			return partitionHeadTimers[indices[0]];
		}

		/**
		 * Add a new timer into the store.
		 */
		boolean add(K key, N namespace, long timestamp) {
			int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, totalKeyGroups);
			int partition = getPartitionForKeyGroup(keyGroupRange, keyGroup, numPartitions);

			Tuple4<Integer, Long, K, N> timer = new Tuple4<>(keyGroup, timestamp, key, namespace);
			insertDB(timer);

			Tuple4<Integer, Long, K, N> headTimer = partitionHeadTimers[indices[0]];
			boolean isNewHead = (headTimer == null || timer.f1 < headTimer.f1);

			// Update the heap if the new timer is the new leader of the partition
			Tuple4<Integer, Long, K, N> partitionHeadTimer = partitionHeadTimers[partition];
			if (partitionHeadTimer == null || timer.f1 < partitionHeadTimer.f1) {
				updatePartitionLeader(partition, timer);
			}

			return isNewHead;
		}

		/**
		 * Remove the given timer from the store.
		 */
		boolean remove(K key, N namespace, long timestamp) {
			int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, totalKeyGroups);
			int partition = getPartitionForKeyGroup(keyGroupRange, keyGroup, numPartitions);

			Tuple4<Integer, Long, K, N> timer = new Tuple4<>(keyGroup, timestamp, key, namespace);
			removeDB(timer);

			Tuple4<Integer, Long, K, N> headTimer = partitionHeadTimers[indices[0]];
			boolean isCurrentHead = (headTimer != null && headTimer.equals(timer));

			// Elect a new leader and update the heap if the deleted timer is the current leader of the partition 
			Tuple4<Integer, Long, K, N> partitionHeadTimer = partitionHeadTimers[partition];
			if (timer.equals(partitionHeadTimer)) {
				Tuple4<Integer, Long, K, N> newPartitionHeadTimer = electPartitionLeader(partition);
				updatePartitionLeader(partition, newPartitionHeadTimer);
			}

			return isCurrentHead;
		}

		/**
		 * Remove and return all timers that are later than the given time.
		 */
		List<Tuple4<Integer, Long, K, N>> peek(long timestamp) {
			List<Tuple4<Integer, Long, K, N>> expiredTimers = new ArrayList<>();

			while (true) {
				int partition = indices[0];

				Tuple4<Integer, Long, K, N> partitionHeadTimer = partitionHeadTimers[partition];
				if (partitionHeadTimer == null || partitionHeadTimer.f1 > timestamp) {
					break;
				}

				Tuple4<Integer, Long, K, N> newPartitionLeader = electPartitionLeader(partition, timestamp, expiredTimers);
				updatePartitionLeader(partition, newPartitionLeader);
			}

			return expiredTimers;
		}

		/**
		 * Return all the timers in the given key group.
		 */
		Set<InternalTimer<K,N>> getTimers(int keyGroup) {
			Set<InternalTimer<K, N>> timers = new HashSet<>();

			RocksIterator iterator = db.newIterator(handle);

			byte[] keyGroupBytes = serializeKeyGroup(keyGroup);
			iterator.seek(keyGroupBytes);

			while (iterator.isValid()) {
				Tuple4<Integer, Long, K, N> rawTimer = deserializeRawTimer(iterator.key());

				if (rawTimer.f0 != keyGroup) {
					break;
				}

				timers.add(new InternalTimer<>(rawTimer.f1, rawTimer.f2, rawTimer.f3));

				iterator.next();
			}

			iterator.close();

			return timers;
		}

		/**
		 * Restore the key group with the given timers
		 */
		void restoreTimers(int keyGroup, Iterable<InternalTimer<K, N>> timers) {
			for (InternalTimer<K, N> timer : timers) {
				Tuple4<Integer, Long, K, N> rawTimer = new Tuple4<>(keyGroup, timer.getTimestamp(), timer.getKey(), timer.getNamespace());

				insertDB(rawTimer);
			}
		}
		
		/** 
		 * Return the number of timers in the given namespace. In the cases 
		 * where the given namespace is null, all timers will be counted.
		 */
		int numTimers(N namespace) {
			int count = 0;

			RocksIterator iterator = db.newIterator(handle);
			iterator.seekToFirst();

			while(iterator.isValid()) {

				boolean matched = true;
				
				if (namespace != null) {
					Tuple4<Integer, Long, K, N> rawTimer = deserializeRawTimer(iterator.key());

					if (rawTimer.f3 != null && !rawTimer.f3.equals(namespace)) {
						matched = false;
					}
				}

				if (matched) {
					count++;
				}

				iterator.next();
			}

			return count;
		}

		private void removeDB(Tuple4<Integer, Long, K, N> rawTimer) {
			try {
				byte[] bytes = serializeRawTimer(rawTimer);
				db.remove(handle, bytes);
			} catch (RocksDBException e) {
				throw new RuntimeException("Error while removing timer from RocksDB.", e);
			}
		}

		private void insertDB(Tuple4<Integer, Long, K, N> rawTimer) {
			try {
				byte[] bytes = serializeRawTimer(rawTimer);
				db.put(handle, bytes, "dummy".getBytes());
			} catch (RocksDBException e) {
				throw new RuntimeException("Error while getting timer from RocksDB.", e);
			}
		}

		/**
		 * Scan the partition to find the leader.
		 * 
		 * @param partition The partition to update
		 * @return The new leader of the partition
		 */
		private Tuple4<Integer, Long, K, N> electPartitionLeader(int partition) {
			return electPartitionLeader(partition, Long.MIN_VALUE, new ArrayList<Tuple4<Integer, Long, K, N>>());
		}

		/**
		 * Scan the partition to find a new leader whose timestamp is larger 
		 * than the given timestamp. All timers who are earlier than the given
		 * timestamp will be removed from the DB.
		 * 
		 * @param partition The partition to update
		 * @param timestamp The expiration timestamp
		 * @param expiredTimers The list to store expired timers
		 * @return The new leader of the partition
		 */
		private Tuple4<Integer, Long, K, N> electPartitionLeader(int partition, long timestamp, List<Tuple4<Integer, Long, K, N>> expiredTimers) {
			Tuple4<Integer, Long, K, N> partitionHeadTimer = null;
			List<Tuple4<Integer, Long, K, N>> partitionExpiredTimers = new ArrayList<>();

			RocksIterator iterator = db.newIterator(handle);

			KeyGroupRange partitionRange = getRangeForPartition(keyGroupRange, partition, numPartitions);
			if (partitionRange == null) {
				throw new IllegalStateException();
			}

			// Start the scanning from the first key group in the partition
			int currentKeyGroup = partitionRange.getStartKeyGroup();
			byte[] currentKeyGroupBytes = serializeKeyGroup(currentKeyGroup);
			iterator.seek(currentKeyGroupBytes);

			while (true) {

				if (!iterator.isValid()) {
					break;
				}

				Tuple4<Integer, Long, K, N> timer = deserializeRawTimer(iterator.key());

				if (!partitionRange.contains(timer.f0)) {
					break;
				}

				if (timer.f1 <= timestamp) {
					partitionExpiredTimers.add(timer);

					iterator.next();
				} else {
					// We come across the first timer who is later than given 
					// instance in the current key group. Use it as the new 
					// leader of the partition if it is earlier than the leaders
					// of previous key groups.
					if (partitionHeadTimer == null || timer.f1 < partitionHeadTimer.f1) {
						partitionHeadTimer = timer;
					}

					// Go to the next key group
					int nextKeyGroup = timer.f0 + 1;
					byte[] nextKeyGroupBytes = serializeKeyGroup(nextKeyGroup);
					iterator.seek(nextKeyGroupBytes);
				}
			}

			iterator.close();

			// Delete expired timers from DB
			expiredTimers.addAll(partitionExpiredTimers);
			for (Tuple4<Integer, Long, K, N> expiredTimer : partitionExpiredTimers) {
				removeDB(expiredTimer);
			}

			return partitionHeadTimer;
		}

		/**
		 * Update the partition's header in the heap.
		 * 
		 * @param partition The partition to update
		 * @param rawTimer The partition's new leader
		 */
		private void updatePartitionLeader(int partition, Tuple4<Integer, Long, K, N> rawTimer) {
			partitionHeadTimers[partition] = rawTimer;

			int currentOrder = orders[partition];
			
			// Walk up in the heap
			while (currentOrder > 0) {
				int currentPartition = indices[currentOrder];
				Tuple4<Integer, Long, K, N> currentTimer = partitionHeadTimers[currentPartition];
				long currentTimestamp = getTimestamp(currentTimer);

				int parentOrder = getParentOrder(currentOrder);
				int parentPartition = indices[parentOrder];
				Tuple4<Integer, Long, K, N> parentTimer = partitionHeadTimers[parentPartition];
				long parentTimestamp = getTimestamp(parentTimer);

				if (currentTimestamp >= parentTimestamp) {
					break;
				} else {

					indices[currentOrder] = parentPartition;
					orders[parentPartition] = currentOrder;

					indices[parentOrder] = currentPartition;
					orders[currentPartition] = parentOrder;

					currentOrder = parentOrder;
				}
			}

			// Walk down in the heap
			while (currentOrder < orders.length) {
				int currentPartition = indices[currentOrder];
				Tuple4<Integer, Long, K, N> currentTimer = partitionHeadTimers[currentPartition];
				long currentTimestamp = getTimestamp(currentTimer);

				int leftChildOrder = getLeftChildOrder(currentOrder);
				int leftChildPartition = leftChildOrder < indices.length ? indices[leftChildOrder] : -1;
				Tuple4<Integer, Long, K, N> leftChildTimer = leftChildPartition == -1 ? null : partitionHeadTimers[leftChildPartition];
				long leftChildTimestamp = getTimestamp(leftChildTimer);

				int rightChildOrder = getRightChildOrder(currentOrder);
				int rightChildPartition = rightChildOrder < indices.length ? indices[rightChildOrder] : -1;
				Tuple4<Integer, Long, K, N> rightChildTimer = rightChildPartition == -1 ? null : partitionHeadTimers[rightChildPartition];
				long rightChildTimestamp = getTimestamp(rightChildTimer);

				if (currentTimestamp > leftChildTimestamp) {
					indices[currentOrder] = leftChildPartition;
					orders[leftChildPartition] = currentOrder;

					indices[leftChildOrder] = currentPartition;
					orders[currentPartition] = leftChildOrder;

					currentOrder = leftChildOrder;
				} else if (currentTimestamp > rightChildTimestamp) {
					indices[currentOrder] = rightChildPartition;
					orders[rightChildPartition] = currentOrder;

					indices[rightChildOrder] = currentPartition;
					orders[currentPartition] = rightChildOrder;

					currentOrder = rightChildOrder;
				} else {
					break;
				}
			}

		}
	}
}

