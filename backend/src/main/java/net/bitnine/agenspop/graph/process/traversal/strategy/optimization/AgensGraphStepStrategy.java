package net.bitnine.agenspop.graph.process.traversal.strategy.optimization;

import net.bitnine.agenspop.graph.process.traversal.step.map.AgensPropertyMapStep;
import net.bitnine.agenspop.graph.process.traversal.step.sideEffect.AgensGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class AgensGraphStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AgensGraphStepStrategy INSTANCE = new AgensGraphStepStrategy();

    private AgensGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        // apply AgensPropertyMapStep to traversal
        applyAgensPropertyMapStepStrategy(traversal);
        // apply AgensGraphStep to traversal
        applyAgensGraphStepStrategy(traversal);

        // for DEBUG
        // System.out.println("AgensGraphStepStrategy = "+traversal.toString());
    }

    public static AgensGraphStepStrategy instance() {
        return INSTANCE;
    }

    ////////////////////////////////////

    private void applyAgensPropertyMapStepStrategy(final Traversal.Admin<?, ?> traversal){
        // traversal 내의 모든 PropertyMapStep 에 대해서 순회
//        for (final PropertyMapStep originStep : TraversalHelper.getStepsOfClass(PropertyMapStep.class, traversal)) {
        for (final PropertyMapStep originStep : TraversalHelper.getStepsOfAssignableClassRecursively(PropertyMapStep.class, traversal)) {
            // PropertyMapStep 을 AgensPropertyMapStep 으로 바꿔치기
            final AgensPropertyMapStep agensStep = new AgensPropertyMapStep(
                    originStep.getTraversal(), originStep.getReturnType(), originStep.getPropertyKeys());
            TraversalHelper.replaceStep(originStep, agensStep, originStep.getTraversal());
        }
    }

    private void applyAgensGraphStepStrategy(final Traversal.Admin<?, ?> traversal){
        // 전체 traversal 에서 graphStep 들 반복
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            // graphStep 을 AgensGraphStep 으로 변경 (바꿔치기)
            final AgensGraphStep<?, ?> agensGraphStep = new AgensGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, agensGraphStep, traversal);

            // 이어지는 과정이 HasStep 또는 NoOpBarrierStep 이면
            Step<?, ?> currentStep = agensGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NotStep || currentStep instanceof NoOpBarrierStep) {
                // GraphStep 으로부터 이어지는 HasStep 인 경우
                if( currentStep instanceof HasStep ){
                    // HasStep 의 모든 filter container 들에 대해서
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        // 지정 id 가 있거나, eq 또는 within 연산자가 있는 경우가 아니면
                        //   agensGraphStep 에 hasContainer 를 추가(??)
                        // for DEBUG
                        // System.out.println("AgensGraphStepStrategy::hasContainer = "+hasContainer.toString());
                        if (!GraphStep.processHasContainerIds(agensGraphStep, hasContainer))
                            agensGraphStep.addHasContainer(hasContainer);
                    }
                    // 현재 단계를 이전 단계로 복사하고, 현재 단계를 전체 traversal 에서 제거 (축약?)
                    // void copyLabels(final Step<?, ?> fromStep, final Step<?, ?> toStep, final boolean moveLabels)
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                else if( currentStep instanceof NotStep ){
                    // ~keyNot(key) => [GraphStep(vertex,[]), NotStep([PropertiesStep([java],value)])]
                    for (Object child : ((NotStep)currentStep).getLocalChildren() ) {
                        if( child instanceof DefaultGraphTraversal && ((DefaultGraphTraversal)child).getSteps().size() == 1 ){
                            Step step = (Step) ((DefaultGraphTraversal)child).getSteps().get(0);
                            // **NOTE: hasNot(key) 는 단 하나의 key 만 가질 수 있음
                            if( step instanceof PropertiesStep && ((PropertiesStep)step).getPropertyKeys().length == 1 ){
                                String keyNot = ((PropertiesStep)step).getPropertyKeys()[0];
                                // Set<String> labels = ((PropertiesStep)step).getLabels();
                                agensGraphStep.addHasContainer(new HasContainer("~key", P.neq(keyNot)));
                                // **NOTE: 나중에 확인 필요!!
                                TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                                traversal.removeStep(currentStep);
                                break;
                            }
                        }
                    }
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }
}

/*
 ** 참고 https://kelvinlawrence.net/book/Gremlin-Graph-Guide.html
 ** 4.19.2. Analyzing where time is spent - introducing profile

g.V().has('region','US-TX').out().has('region','US-CA').
                            out().has('country','DE').profile()
==>
Step                                       Count  Traversers  Time (ms)    % Dur
===============================================================================
TinkerGraphStep(vertex,[region.eq(US-TX)])   26          26      1.810     9.71
VertexStep(OUT,vertex)                      701         701      0.877     4.70
HasStep([region.eq(US-CA)])                  47          47      0.561     3.01
VertexStep(OUT,vertex)                     3464        3464     12.035    64.54
NoOpBarrierStep(2500)                      3464         224      3.157    16.93
HasStep([country.eq(DE)])                    59           4      0.206     1.11
   >TOTAL

 */