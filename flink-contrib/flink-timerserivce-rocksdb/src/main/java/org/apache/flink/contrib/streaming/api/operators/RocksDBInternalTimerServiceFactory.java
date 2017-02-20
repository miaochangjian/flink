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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimerServiceFactory;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBInternalTimerServiceFactory implements InternalTimerServiceFactory {

	private static Logger LOG = LoggerFactory.getLogger(RocksDBInternalTimerService.class);
	
	@Override
	public InternalTimerService<?, ?> createInternalTimerService(
			int totalKeyGroups,
			KeyGroupRange keyGroupRange,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService, 
			Configuration configuration) throws Exception {
		
		String dbPath = configuration.getString("timerservice.rocksdb.path", null);
		if (dbPath == null) {
			LOG.warn("The path for rocksdb timer service is not specified. The default path (${java.io.tmpdir}) will be used.");
			
			dbPath = System.getProperty("java.io.tmpdir");
		}
	
		return new RocksDBInternalTimerService<>(totalKeyGroups, keyGroupRange, keyContext, processingTimeService, new Path(dbPath));
	}
}

