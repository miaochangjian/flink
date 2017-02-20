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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class RocksDBInternalTimerServiceFactoryTest {
	
	@Test
	public void testTimerServiceCreation() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.TIMER_SERVICE, 
				"org.apache.flink.contrib.streaming.api.operators.RocksDBInternalTimerServiceFactory");

		TaskManagerRuntimeInfo taskManagerInfo = mock(TaskManagerRuntimeInfo.class);
		when(taskManagerInfo.getConfiguration()).thenReturn(configuration);
				
		RuntimeEnvironment env = mock(RuntimeEnvironment.class);
		when(env.getTaskManagerInfo()).thenReturn(taskManagerInfo);
		when(env.getMetricGroup()).thenReturn(mock(TaskMetricGroup.class));
		
		StreamTask streamTask = mock(StreamTask.class);
		when(streamTask.getEnvironment()).thenReturn(env);
		
		ProcessingTimeService processingTimeService = mock(ProcessingTimeService.class);
		
		StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
		when(runtimeContext.getProcessingTimeService()).thenReturn(processingTimeService);
		
		AbstractKeyedStateBackend keyedStateBackend = mock(AbstractKeyedStateBackend.class);
		when(keyedStateBackend.getNumberOfKeyGroups()).thenReturn(1);
		when(keyedStateBackend.getKeyGroupRange()).thenReturn(new KeyGroupRange(0, 1));
		when(keyedStateBackend.getKeySerializer()).thenReturn(IntSerializer.INSTANCE);
		
		TestOperator operator = new TestOperator();
		
		Whitebox.setInternalState(operator, "container", streamTask);
		Whitebox.setInternalState(operator, "runtimeContext", runtimeContext);
		Whitebox.setInternalState(operator, "keyedStateBackend", keyedStateBackend);
		
		operator.open();
		
		InternalTimerService<Integer, VoidNamespace> timerService = operator.getInternalTimerService(
					"test-timers",
					VoidNamespaceSerializer.INSTANCE,
					operator);
		
		assertTrue(timerService instanceof RocksDBInternalTimerService);
	}

	class TestOperator extends AbstractStreamOperator<String> implements Triggerable<Integer, VoidNamespace> {
		
		@Override
		public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
			
		}

		@Override
		public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
			
		}
	}
}

