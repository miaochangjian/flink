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

package com.alibaba.blink.odps.externalcatalog;

import com.alibaba.blink.odps.schema.ODPSColumn;
import com.alibaba.blink.odps.schema.ODPSTableSchema;
import com.alibaba.blink.odps.type.ODPSType;
import com.alibaba.blink.odps.util.ODPSUtil;
import com.aliyun.odps.Group;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.PartitionNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.catalog.DataSchema;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogDatabase;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalogTablePartition;
import org.apache.flink.table.catalog.TableIdentifier;
import org.apache.flink.table.plan.stats.TablePartitionStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for connect odps as {@link ExternalCatalog}.
 * Note: it's a read only service, cannot provide create/drop/alter operation of odps
 * table/database/partition.
 */
public class OdpsExternalCatalog implements ExternalCatalog {


	private static final Logger LOGGER = LoggerFactory.getLogger(OdpsExternalCatalog.class);
	private final String aliyunAccount;
	private final Odps odps;
	private final ImmutableMap<String, String> odpsConf;
	private final OdpsStatsProvider statsCollector;

	private OdpsExternalCatalog(
			String accessId,
			String accessKey,
			String endpoint,
			String aliyunAccount) {
		this.aliyunAccount = aliyunAccount;
		this.odps = ODPSUtil.initOdps(accessId, accessKey, endpoint, null);
		odpsConf = ImmutableMap.of(
				"accessId", accessId,
				"accessKey", accessKey,
				"endpoint", endpoint);
		this.statsCollector = new OdpsStatsProvider(odps);
	}

	@Override
	public ExternalCatalogTablePartition getPartition(
			String dbName,
			String tableName,
			Map<String, String> partSpec)
			throws DatabaseNotExistException, TableNotExistException, PartitionNotExistException {
		PartitionSpec odpsPartitionSpec = PartitionSpecConverter.fromMap(partSpec);
		TablePartitionStats partitionStats =
				statsCollector.requestStatsOfTablePartition(dbName, tableName, odpsPartitionSpec);
		return new ExternalCatalogTablePartition(
				partSpec,
				ImmutableMap.<String, String>of(),
				partitionStats);
	}

	@Override
	public List<Map<String, String>> listPartitionSpec(String dbName,
			String tableName) throws DatabaseNotExistException, TableNotExistException {
		Table odpsTable = odps.tables().get(dbName, tableName);
		// notes: a RuntimeException will happen if the table is not a partitioned table
		return FluentIterable
				.from(odpsTable.getPartitions())
				.transform(
						new Function<Partition, Map<String, String>>() {

							@Override
							public Map<String, String> apply(Partition partition) {
								PartitionSpec partitionSpec = partition.getPartitionSpec();
								return PartitionSpecConverter.toMap(partitionSpec);
							}
						}).toList();
	}

	@Override
	public ExternalCatalogTable getTable(String dbName,
			String tableName) throws DatabaseNotExistException, TableNotExistException {
		Table odpsTable = odps.tables().get(dbName, tableName);
		TableSchema schema = odpsTable.getSchema();
		ODPSTableSchema tableSchema =
				new ODPSTableSchema(
						schema.getColumns(),
						schema.getPartitionColumns(),
						odpsTable.isVirtualView());
		DataSchema dataSchema = parseOdpsTableTableSchema(tableSchema);
		Set<String> partitionColumns = null;
		TableStats tableStats = null;
		if (tableSchema.isPartition() || tableSchema.isView()) {
			partitionColumns = parsePartitionColumns(tableSchema);
		} else {
			partitionColumns = ImmutableSet.of();
			tableStats = this.statsCollector.requestStatsOfNonPartitionTable(dbName, tableName);
		}
		ExternalCatalogTable table = new ExternalCatalogTable(
				new TableIdentifier(dbName, tableName),
				"odps",
				dataSchema,
				odpsConf,
				tableStats,
				null,
				partitionColumns,
				tableSchema.isPartition(),
				odpsTable.getCreatedTime().getTime(),
				-1L);
		return table;
	}

	@Override
	public List<String> listTables(String dbName) throws DatabaseNotExistException {
		// notes: a RuntimeException will happen if has no privilege 'odps:List' on the project
		return ImmutableList.copyOf(Iterators.transform(odps.tables().iterator(dbName),
				new Function<Table, String>() {
					@Override
					public String apply(Table table) {
						return table.getName();
					}
				}));
	}

	@Override
	public ExternalCatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException {
		try {
			Project p = odps.projects().get(dbName);
			p.reload();
			return new ExternalCatalogDatabase(dbName, ImmutableMap.<String, String>of());
		} catch (NoSuchObjectException e) {
			LOGGER.warn("no such project {}", dbName, e);
			throw new DatabaseNotExistException(dbName, e);
		} catch (OdpsException e) {
			LOGGER.warn("error happened when get project {}", dbName, e);
			throw new RuntimeException("error happened when get project " + dbName, e);
		}
	}

	/**
	 * @return If aliyunAccount is null, return all odps project lists;
	 * else return odps project lists which account has privilege on
	 */
	@Override
	public List<String> listDatabases() {
		Iterator<Project> projectItr = Group.getProjects(null, null, aliyunAccount, odps);
		return ImmutableList.copyOf(Iterators.transform(projectItr,
				new Function<Project, String>() {
					@Override
					public String apply(Project project) {
						return project.getName();
					}
				}));
	}

	private DataSchema parseOdpsTableTableSchema(ODPSTableSchema tableSchema) {
		int columnsNum = tableSchema.getColumns().size();
		String[] returnFieldsName = new String[columnsNum];
		TypeInformation<?>[] returnFieldsType = new TypeInformation<?>[columnsNum];
		for (int idx = 0; idx < columnsNum; idx++) {
			ODPSColumn column = tableSchema.getColumns().get(idx);
			returnFieldsName[idx] = column.getName();
			returnFieldsType[idx] = ODPSType.valueOf(column.getType().name()).toFlinkType();
		}
		return new DataSchema(returnFieldsType, returnFieldsName);
	}

	private Set<String> parsePartitionColumns(ODPSTableSchema schema) {
		return FluentIterable.from(schema.getColumns())
				.filter(new Predicate<ODPSColumn>() {
					@Override
					public boolean apply(@Nullable ODPSColumn column) {
						return column.isPartition();
					}
				})
				.transform(new Function<ODPSColumn, String>() {
					@Override
					public String apply(ODPSColumn column) {
						return column.getName();
					}
				}).toSet();
	}

	public static class Builder {
		// required params
		private String accessId;
		private String accessKey;
		private String endpoint;
		// optional params
		private String aliyunAccount;

		public Builder() {

		}

		public Builder setAccessId(String accessId) {
			checkArgument(
					StringUtils.isNotBlank(accessId),
					"input accessId cannot be whitespace or null!");
			this.accessId = accessId;
			return this;
		}

		public Builder setAccessKey(String accessKey) {
			checkArgument(
					StringUtils.isNotBlank(accessKey),
					"input accessKey cannot be whitespace or null!");
			this.accessKey = accessKey;
			return this;
		}

		public Builder setEndpoint(String endpoint) {
			checkArgument(
					StringUtils.isNotBlank(endpoint),
					"input endpoint cannot be whitespace or null!");
			this.endpoint = endpoint;
			return this;
		}

		public Builder setAliyunAccount(String aliyunAccount) {
			checkArgument(
					StringUtils.isNotBlank(aliyunAccount),
					"input aliyunAccount cannot be whitespace or null!");
			this.aliyunAccount = aliyunAccount;
			return this;
		}

		public OdpsExternalCatalog build() {
			checkNotNull(accessId, "accessId cannot be null!");
			checkNotNull(accessKey, "accessKey cannot be null!");
			checkNotNull(endpoint, "endpoint cannot be null!");
			return new OdpsExternalCatalog(
					accessId,
					accessKey,
					endpoint,
					aliyunAccount);
		}
	}
}
