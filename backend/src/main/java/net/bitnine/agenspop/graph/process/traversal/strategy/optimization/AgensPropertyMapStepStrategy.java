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
// 최상위 배열의 Step 가져올 수 있다
[GraphStep(vertex,[]), PropertyMapStep(value)]

// 이처럼 안쪽에 들어간 Step 은 가져오지 못한다
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

        // traversal 내의 모든 PropertyMapStep 에 대해서 순회
//        for (final PropertyMapStep originStep : TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal)) {
        for (final PropertyMapStep originStep : TraversalHelper.getStepsOfAssignableClassRecursively(PropertyMapStep.class, traversal)) {
            // PropertyMapStep 을 AgensPropertyMapStep 으로 바꿔치기
            final AgensPropertyMapStep agensStep = new AgensPropertyMapStep(
                    originStep.getTraversal(), originStep.getReturnType(), originStep.getPropertyKeys());
            TraversalHelper.replaceStep(originStep, agensStep, originStep.getTraversal());
        }
        // for DEBUG
        System.out.println("AgensPropertyMapStepStrategy::traversal = "+traversal.toString());
    }

    // 마지막에 적용되는 최적화 전략
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(AgensGraphStepStrategy.class);
    }

    public static AgensPropertyMapStepStrategy instance() {
        return INSTANCE;
    }
}
