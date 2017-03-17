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

package org.apache.flink.table.calcite

import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.plan.{Contexts, ConventionTraitDef, RelOptCluster, RelOptPlanner}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.{DefaultRelMetadataProvider, JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.tools.FrameworkConfig
import org.apache.flink.table.plan.cost.FlinkDefaultRelMetadataProvider

/**
  * Flink specific [[RelOptCluster]] that changes the default metadataProvider from [[DefaultRelMetadataProvider]] to a [[FlinkDefaultRelMetadataProvider]].
  */
class FlinkRelOptClusterBuilder(
    planner: RelOptPlanner,
    rexBuilder: RexBuilder) {

   private val relOptCluster: RelOptCluster = {
     val cluster = RelOptCluster.create(planner, rexBuilder)
     cluster.setMetadataProvider(FlinkDefaultRelMetadataProvider.INSTANCE)
     // just set metadataProvider is not enough, see
     // https://www.mail-archive.com/dev@calcite.apache.org/msg00930.html
     RelMetadataQuery.THREAD_PROVIDERS.set(
       JaninoRelMetadataProvider.of(cluster.getMetadataProvider))
     cluster
   }

   def getTypeFactory: FlinkTypeFactory =
     relOptCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

   def getPlanner: RelOptPlanner = relOptCluster.getPlanner

   def getRelOptCluster: RelOptCluster = relOptCluster
}

object FlinkRelOptClusterBuilder {

  def create(config: FrameworkConfig): FlinkRelOptClusterBuilder = {
    // create Flink type factory
    val typeSystem = config.getTypeSystem
    val typeFactory = new FlinkTypeFactory(typeSystem)
    // create context instances with Flink type factory
    val planner = new VolcanoPlanner(config.getCostFactory, Contexts.empty())
    planner.setExecutor(config.getExecutor)
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    new FlinkRelOptClusterBuilder(planner, new RexBuilder(typeFactory))
  }

}
