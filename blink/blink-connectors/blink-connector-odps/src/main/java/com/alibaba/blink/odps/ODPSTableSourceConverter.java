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

package com.alibaba.blink.odps;

import com.google.common.collect.ImmutableSet;
import org.apache.flink.table.annotation.TableType;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.TableSourceConverter;

import java.util.Map;
import java.util.Set;

@TableType(value = "odps")
public class ODPSTableSourceConverter implements TableSourceConverter<ODPSTableSource> {

	private static final Set<String> REQUIRED_PARAMS =
			ImmutableSet.of("accessId", "accessKey", "endpoint");

	@Override
	public Set<String> requiredProperties() {
		return REQUIRED_PARAMS;
	}

	@Override
	public ODPSTableSource fromExternalCatalogTable(ExternalCatalogTable externalCatalogTable) {
		Map<String, String> properties = externalCatalogTable.properties();
		String project = externalCatalogTable.identifier().database();
		String table = externalCatalogTable.identifier().table();
		ODPSTableSource odpsTableSource = ODPSTableSource.builder()
				.setAccessId(properties.get("accessId"))
				.setAccessKey(properties.get("accessKey"))
				.setEndpoint(properties.get("endpoint"))
				.setProject(project)
				.setTable(table)
				.build();
		return odpsTableSource;
	}
}
