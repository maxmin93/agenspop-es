package net.bitnine.agenspop.graph.process.traversal.strategy.optimization;


import net.bitnine.agenspop.graph.process.traversal.step.map.AgensCountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This strategy will do a direct {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper#getVertices}
 * size call if the traversal is a count of the vertices and edges of the graph or a one-to-one map chain thereof.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @example <pre>
 * g.V().count()               // is replaced by TinkerCountGlobalStep
 * g.V().map(out()).count()    // is replaced by TinkerCountGlobalStep
 * g.E().label().count()       // is replaced by TinkerCountGlobalStep
 * </pre>
 */
public final class AgensGraphCountStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AgensGraphCountStrategy INSTANCE = new AgensGraphCountStrategy();

    private AgensGraphCountStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep) || TraversalHelper.onGraphComputer(traversal))
            return;

        final List<Step> steps = traversal.getSteps();
        if (steps.size() < 2 ||                                         // 스텝이 2 미만
            !(steps.get(0) instanceof GraphStep) ||                     // 시작이 GraphStep 이 아님
            0 != ((GraphStep) steps.get(0)).getIds().length ||          // 탐색할 Ids 가 설정되었거나
            !(steps.get(steps.size() - 1) instanceof CountGlobalStep)   // 마지막 스텝이 count 가 아니면
        ) return;                                                       // => 건너뛰기

        // graph step 이후로 순회
        // => count step 을 수행할 traversal 이 아니면 건너뛰기
        for (int i = 1; i < steps.size() - 1; i++) {
            final Step current = steps.get(i);
            if (!(//current instanceof MapStep ||  // MapSteps will not necessarily emit an element as demonstrated in https://issues.apache.org/jira/browse/TINKERPOP-1958
                    current instanceof IdentityStep ||
                    current instanceof NoOpBarrierStep ||
                    current instanceof CollectingBarrierStep) ||
                    (current instanceof TraversalParent &&
                            TraversalHelper.anyStepRecursively(s -> (s instanceof SideEffectStep || s instanceof AggregateStep), (TraversalParent) current)))
                return;
        }

        // 반환 타입 클래스
        final Class<? extends Element> elementClass = ((GraphStep<?, ?>) steps.get(0)).getReturnClass();
        // traversal 의 모든 과정 비우고
        TraversalHelper.removeAllSteps(traversal);
        // AgensCountGlobalStep 만 넣기
        traversal.addStep(new AgensCountGlobalStep<>(traversal, elementClass));
    }

    // 마지막에 적용되는 최적화 전략
    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(AgensGraphStepStrategy.class);
    }

    public static AgensGraphCountStrategy instance() {
        return INSTANCE;
    }
}