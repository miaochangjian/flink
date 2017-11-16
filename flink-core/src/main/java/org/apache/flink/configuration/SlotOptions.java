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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to the slot.
 */
@PublicEvolving
public class SlotOptions {

	/**
	 * The timeout of a slot allocation.
	 */
	public static final ConfigOption<String> SLOT_ALLOCATION_TIMEOUT =
		key("slot.allocation.timeout")
			.defaultValue("10 min");

	/**
	 * The timeout of a slot request to resource manager.
	 */
	public static final ConfigOption<String> SLOT_REQUEST_RESOURCE_MANAGER_TIMEOUT =
		key("slot.request.resourcemanager.timeout")
			.defaultValue("10 s");

	/**
	 * The timeout of a slot allocation to resource manager.
	 */
	public static final ConfigOption<String> SLOT_ALLOCATION_RESOURCE_MANAGER_TIMEOUT =
		key("slot.allocation.resourcemanager.timeout")
			.defaultValue("5 min");

	/**
	 * The timeout of a idle slot in slot pool.
	 * If timeout happens, the idle slot in slot pool would be returned to resource manager.
	 */
	public static final ConfigOption<String> SLOT_POOL_IDEL_TIMEOUT =
		key("slot.pool.idle.timeout")
			.defaultValue("5 min");
}
