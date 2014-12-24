/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package eu.inn.samza.mesos

import eu.inn.samza.mesos.MesosConfig.Config2Mesos
import org.apache.mesos.Protos.{OfferID, Offer, TaskInfo}
import org.apache.samza.config.Config
import org.apache.samza.container.{TaskName, TaskNamesToSystemStreamPartitions}
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.util.{Logging, Util}

import scala.collection.mutable
import scala.collection.JavaConversions._

/*
Let's be very clear about terminology:
  - Samza task
    - the instance of StreamTask that processes messages from a single stream partition
    - Samza determines how many Samza tasks there are for the Samza job, and the assignment of Samza tasks to Samza containers
  - Samza container
    - the JVM process that contains 1 or more Samza tasks
    - the number of Samza containers for the Samza job is configurable (mesos.executor.count), defaults to 1
  - Samza job: 
    - the high-level processing, actually carried out by individual Samza containers & Samza tasks
    - a Samza job runs in Mesos as a Mesos framework, which schedules Mesos tasks (Samza containers) to run out in the Mesos cluster
  - Mesos task
    - 1 Samza container running on a Mesos slave machine
    - Mesos task = Samza container
    - So 1 Samza job = 1 or more Mesos tasks
  - Mesos framework
    - a Mesos scheduler that schedules Mesos tasks to run out in the Mesos cluster
    - a Mesos executor that actually runs each Mesos task
    - Mesos framework = Samza job

To summarize:
  - Samza job = Mesos framework
  - Samza container = Mesos task
  - Samza task has no Mesos equivalent

Note that both Mesos and Samza have a thing called "task", so need to qualify that term to avoid confusion
*/

class SamzaSchedulerState(config: Config) extends Logging {
  var currentStatus: ApplicationStatus = New //TODO should this get updated to other values at some point? possible values are: New, Running, SuccessfulFinish, UnsuccessfulFinish

  val samzaContainerCount: Int = config.getTaskCount.getOrElse({
    info(s"No ${MesosConfig.EXECUTOR_TASK_COUNT} specified. Defaulting to one Samza container (i.e. one Mesos task).")
    1
  })
  debug(s"Samza container (i.e. Mesos task) count: ${samzaContainerCount}")

  val samzaContainerIds = (0 until samzaContainerCount).toSet
  debug(s"Samza container IDs: ${samzaContainerIds}")

  val samzaContainerIdToSSPTaskNames: Map[Int, TaskNamesToSystemStreamPartitions] =
    Util.assignContainerToSSPTaskNames(config, samzaContainerCount)
  debug(s"Samza container ID to SSP task names: ${samzaContainerIdToSSPTaskNames}")

  val samzaTaskNameToChangeLogPartitionMapping: Map[TaskName, Int] =
    Util.getTaskNameToChangeLogPartitionMapping(config, samzaContainerIdToSSPTaskNames)
  debug(s"Samza task name to changelog partition mapping: ${samzaTaskNameToChangeLogPartitionMapping}")

  val mesosTasks: Map[String, MesosTask] = samzaContainerIds.map { containerId => 
    val task = new MesosTask(config, this, containerId)
    (task.getMesosTaskId, task)
  }.toMap
  debug(s"Mesos task IDs: ${mesosTasks.keys}")

  //TODO need task state transition methods here, and maybe don't expose these public mutable sets
  val unclaimedTasks: mutable.Set[String] = mutable.Set(mesosTasks.keys.toSeq: _*)
  val pendingTasks: mutable.Set[String] = mutable.Set()
  val runningTasks: mutable.Set[String] = mutable.Set()

  def filterTasks(ids: Iterable[String]): Set[MesosTask] = mesosTasks.filterKeys(ids.contains).values.toSet

  def dump() = {
    info(s"Tasks state: unclaimed: ${unclaimedTasks.size}, pending: ${pendingTasks.size}, running: ${runningTasks.size}")
  }
}