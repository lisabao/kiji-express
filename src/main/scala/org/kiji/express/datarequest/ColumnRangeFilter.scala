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

import org.kiji.express.avro.ColumnRangeFilterSpec
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.KijiColumnRangeFilter

case class ColumnRangeFilter(minQualifier: String, minIncluded: Boolean, maxQualifier: String,
    maxIncluded: Boolean) extends ExpressColumnFilter {

  def getKijiColumnFilter(): KijiColumnFilter = new KijiColumnRangeFilter(minQualifier, minIncluded,
      maxQualifier, maxIncluded)

  def getAvroColumnFilter(): ColumnRangeFilterSpec = ColumnRangeFilterSpec.newBuilder()
        .setMinQualifier(minQualifier).setMinIncluded(minIncluded)
        .setMaxQualifier(maxQualifier).setMaxIncluded(maxIncluded)
        .build()
}
