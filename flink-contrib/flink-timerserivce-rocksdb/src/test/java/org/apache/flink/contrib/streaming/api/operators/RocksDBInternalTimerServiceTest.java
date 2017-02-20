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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimerServiceTestBase;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

/**
 * Tests for {@link RocksDBInternalTimerService}.
 */
@RunWith(Parameterized.class)
public class RocksDBInternalTimerServiceTest extends InternalTimerServiceTestBase {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public RocksDBInternalTimerServiceTest(int startKeyGroup, int endKeyGroup, int maxParallelism) {
		super(startKeyGroup, endKeyGroup, maxParallelism);
	}

	@Override
	protected InternalTimerService<Integer, String> getInternalTimerService(
			int totalKeyGroups,
			KeyGroupRange keyGroupRange, 
			KeyContext keyContext, 
			ProcessingTimeService processingTimeService) {

		String dbPath;
		
		try {
			dbPath = tempFolder.newFolder().getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException("Error while creating the temporary directory.", e);
		}

		return new RocksDBInternalTimerService<>(totalKeyGroups, keyGroupRange, keyContext, processingTimeService, new Path(dbPath));
	}
}