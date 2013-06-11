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

import org.kiji.express.avro.AndFilterSpec
import org.kiji.express.avro.ColumnRangeFilterSpec
import org.kiji.express.avro.RegexQualifierFilterSpec
import org.kiji.express.avro.OrFilterSpec
import org.kiji.schema.filter.KijiColumnFilter

/**
 *
 */
trait ExpressColumnFilter {
  /**
   * Returns a KijiColumnFilter that corresponds to the Express column filter.
   */
  def getKijiColumnFilter(): KijiColumnFilter

  /**
   * Returns an AvroRecord that describes this Express column filter.
   */
  def getAvroColumnFilter(): AnyRef

  /**
   * A utility method that allows us to recursively translate And and Or express filters
   * into the corresponding Avro representation.
   *
   * @param filter to translate into Avro
   * @return The description of a filter using Avro records.
   */
  private[express] def expressToAvroFilter(filter: ExpressColumnFilter): AnyRef = {
    filter match {
      case regexFilter: RegexQualifierFilter =>
        RegexQualifierFilterSpec.newBuilder()
          .setRegex(regexFilter.regex)
          .build()
      case rangeFilter: ColumnRangeFilter =>
        ColumnRangeFilterSpec.newBuilder()
          .setMinQualifier(rangeFilter.minQualifier)
          .setMinIncluded(rangeFilter.minIncluded)
          .setMaxQualifier(rangeFilter.maxQualifier)
          .setMaxIncluded(rangeFilter.maxIncluded)
          .build()
      case andFilter: AndFilter =>
        val expFilterList: List[AnyRef] = andFilter.filtersList map { expressToAvroFilter _ }
        AndFilterSpec.newBuilder().setFilters(expFilterList.asJava).build()
      case orFilter: OrFilter =>
        val filterList: List[AnyRef] = orFilter.filtersList map { expressToAvroFilter _ }
        OrFilterSpec.newBuilder().setFilters(filterList.asJava)
      }
    }
  }
