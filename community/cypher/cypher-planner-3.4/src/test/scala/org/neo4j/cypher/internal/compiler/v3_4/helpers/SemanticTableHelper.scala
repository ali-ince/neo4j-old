/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_4.helpers

import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable

object SemanticTableHelper {
  implicit class RichSemanticTable(table: SemanticTable) {
    def transplantResolutionOnto(target: SemanticTable) =
      target.copy(
        resolvedLabelIds = table.resolvedLabelNames,
        resolvedPropertyKeyNames = table.resolvedPropertyKeyNames,
        resolvedRelTypeNames = table.resolvedRelTypeNames
      )
  }
}
