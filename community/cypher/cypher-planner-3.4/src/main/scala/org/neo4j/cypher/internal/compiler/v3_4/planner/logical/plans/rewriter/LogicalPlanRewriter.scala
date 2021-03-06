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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.phases.{CompilerContext, LogicalPlanState}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.v3_4.phases.{Condition, Phase}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.Rewriter
import org.neo4j.cypher.internal.util.v3_4.attribution.Attributes

/*
 * Rewriters that live here are required to adhere to the contract of
 * receiving a valid plan and producing a valid plan. It should be possible
 * to disable any and all of these rewriters, and still produce correct behavior.
 */
case class PlanRewriter(rewriterSequencer: String => RewriterStepSequencer) extends LogicalPlanRewriter {
  override def description: String = "optimize logical plans using heuristic rewriting"

  override def postConditions: Set[Condition] = Set.empty

  override def instance(context: CompilerContext, solveds: Solveds, cardinalities: Cardinalities, otherAttributes: Attributes) = fixedPoint(rewriterSequencer("LogicalPlanRewriter")(
    fuseSelections,
    unnestApply(solveds, otherAttributes.withAlso(cardinalities)),
    cleanUpEager(solveds, otherAttributes.withAlso(cardinalities)),
    simplifyPredicates,
    unnestOptional,
    predicateRemovalThroughJoins(solveds, cardinalities, otherAttributes),
    removeIdenticalPlans(otherAttributes.withAlso(cardinalities, solveds)),
    pruningVarExpander,
    useTop,
    simplifySelections
  ).rewriter)
}

trait LogicalPlanRewriter extends Phase[CompilerContext, LogicalPlanState, LogicalPlanState] {
  override def phase: CompilationPhase = LOGICAL_PLANNING

  def instance(context: CompilerContext, solveds: Solveds, cardinalities: Cardinalities, otherAttributes: Attributes): Rewriter

  override def process(from: LogicalPlanState, context: CompilerContext): LogicalPlanState = {
    val idGen = context.logicalPlanIdGen
    val otherAttributes = Attributes(idGen)
    val rewritten = from.logicalPlan.endoRewrite(instance(context, from.solveds, from.cardinalities, otherAttributes))
    from.copy(maybeLogicalPlan = Some(rewritten))
  }
}
