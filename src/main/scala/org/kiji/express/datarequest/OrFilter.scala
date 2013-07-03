/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.datarequest

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.kiji.express.avro.OrFilterSpec
import org.kiji.schema.filter.Filters
import org.kiji.schema.filter.KijiColumnFilter

case class OrFilter(filtersList: List[ExpressColumnFilter]) extends ExpressColumnFilter {
  /**
   * Returns a concrete KijiColumnFilter that implements this express column filter.
   */
  def getKijiColumnFilter(): KijiColumnFilter = {
    val schemaFilters = filtersList.map{filter: ExpressColumnFilter => filter.getKijiColumnFilter()}
    Filters.or(schemaFilters.toArray : _*)
  }
}
