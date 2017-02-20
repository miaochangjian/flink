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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * A factory to create a customized internal timer service. The configuration
 * of the task manager will be passed to the factory  so that the factory can
 * read needed configuration values.
 * 
 * The factory must provide a default constructor without any parameter.
 */
public interface InternalTimerServiceFactory {
	
	/**
	 * Creates the timer service, optionally using the given configuration.
	 * 
	 * @param totalKeyGroups The total number of key groups in the job.
	 * @param keyGroupRange The key groups assigned to the task.   
	 * @param keyContext The key context
	 * @param processingTimeService The processing timer service   
	 * @param config The Flink configuration (loaded by the TaskManager).
	 * @return The created internal timer service.
	 * 
	 * @throws Exception Exceptions during instantiation can be forwarded.
	 */
	public InternalTimerService<?, ?> createInternalTimerService(
			int totalKeyGroups,
			KeyGroupRange keyGroupRange,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService, 
			Configuration config) throws Exception;
}