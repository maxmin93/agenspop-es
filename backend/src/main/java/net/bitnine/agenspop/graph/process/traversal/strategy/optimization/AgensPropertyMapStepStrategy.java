package net.bitnine.agenspop.graph.process.traversal.strategy.optimization;

import net.bitnine.agenspop.graph.process.traversal.step.map.AgensCountGlobalStep;
import net.bitnine.agenspop.graph.process.traversal.step.map.AgensPropertyMapStep;
import net.bitnine.agenspop.graph.process.traversal.step.sideEffect.AgensGraphStep;
import net.bitnine.agenspop.graph.process.traversal.step.sideEffect.AgensVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.PropertyType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class AgensPropertyMapStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AgensPropertyMapStepStrategy INSTANCE = new AgensPropertyMapStepStrategy();

    private AgensPropertyMapStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?,?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep) || TraversalHelper.onGraphComputer(traversal))
            return;

/*
// ????????? ????????? Step ????????? ??? ??????
[GraphStep(vertex,[]), PropertyMapStep(value)]

// ????????? ????????? ????????? Step ??? ???????????? ?????????
[
    GraphStep(edge,[])
    , HasStep([~label.eq(knows)])@[a]
    , RangeGlobalStep(0,2)
    , ProjectStep([a],[[IdentityStep]])
    , ProjectStep([a],[[
        SelectOneStep(last,a), ProjectStep([cypher.element, cypher.inv, cypher.outv],[
            [PropertyMapStep(value)]
            , [EdgeVertexStep(IN), IdStep]
            , [EdgeVertexStep(OUT), IdStep]
        ])
    ]])
]
*/

        // traversal ?????? ?????? PropertyMapStep ??? ????????? ??????
//        for (final PropertyMapStep originStep : TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal)) {
        for (final PropertyMapStep originStep : TraversalHelper.getStepsOfAssignableClassRecursively(PropertyMapStep.class, traversal)) {
            // PropertyMapStep ??? AgensPropertyMapStep ?????? ????????????
            final AgensPropertyMapStep agensStep = new AgensPropertyMapStep(
                    originStep.getTraversal(), originStep.getReturnType(), originStep.getPropertyKeys());
            TraversalHelper.replaceStep(originStep, agensStep, originStep.getTraversal());
        }
        // for DEBUG
        System.out.println("AgensPropertyMapStepStrategy::traversal = "+traversal.toString());
    }

    // ???????????? ???????????? ????????? ??????
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(AgensGraphStepStrategy.class);
    }

    public static AgensPropertyMapStepStrategy instance() {
        return INSTANCE;
    }
}
