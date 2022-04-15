package net.bitnine.agenspop.graph.process.traversal.step.sideEffect;


import net.bitnine.agenspop.graph.structure.AgensGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 */
public final class AgensGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public AgensGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);

        // we used to only setIteratorSupplier() if there were no ids OR the first id was instanceof Element,
        // but that allowed the filter in g.V(v).has('k','v') to be ignored.  this created problems for
        // PartitionStrategy which wants to prevent someone from passing "v" from one TraversalSource to
        // another TraversalSource using a different partition
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        final AgensGraph graph = (AgensGraph) this.getTraversal().getGraph().get();
        // ids are present, filter on them first
        if (null == this.ids)
            return Collections.emptyIterator();
        // if hasStep exists, then call optimized edges()
        if( hasContainers.size() > 0 ){
            return this.iteratorList(graph.edges(hasContainers, this.ids));
        }
        else return this.iteratorList(graph.edges(this.ids));
    }

    private Iterator<? extends Vertex> vertices() {
        final AgensGraph graph = (AgensGraph) this.getTraversal().getGraph().get();
        // ids are present, filter on them first
        if (null == this.ids) return Collections.emptyIterator();
        // if hasStep exists, then call optimized vertices()
        if( hasContainers.size() > 0 ){
            return this.iteratorList(graph.vertices(hasContainers, this.ids));
        }
        return this.iteratorList(graph.vertices(this.ids));
    }

//    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
//        final Set<String> indexedKeys = ((AgensGraph) this.getTraversal().getGraph().get()).getIndexedKeys(indexedClass);
//
//        final Iterator<HasContainer> itty = IteratorUtils.filter(hasContainers.iterator(),
//                c -> c.getPredicate().getBiPredicate() == Compare.eq && indexedKeys.contains(c.getKey()));
//        return itty.hasNext() ? itty.next() : null;
//
//    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    private boolean containersTestAll(final Element element, final List<HasContainer> hasContainers) {
        for (final HasContainer hasContainer : hasContainers) {
            // for DEBUG
            // System.out.println("  **hasTest("+element.getClass().getSimpleName()+") : "
            //         +hasContainer.toString()+" -> "+hasContainer.test(element));

            // **NOTE
            // AgensGraphStep 에서 넘기는 element 는 Vertex 또는 Edge 임
            // 그 외의 테스트에 대해서는 검토 필요!!
            //
            // Solution1) AgensGraph.getOptimizedType() 에서 최적화된 hasContainer 삭제
            //
            // ex) 예를 들어 hasKey 테스트를 수행하면 Vertex/Edge 에 대해서는 검사가 안되기 때문에
            //      false 를 반환하게 됨
            if (!hasContainer.test(element)) return false;
        }
        return true;
    }

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            final E e = iterator.next();
            if (containersTestAll(e, this.hasContainers))
                list.add(e);
        }
        return list.iterator();
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        // 하나 이상이면
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                // 재귀호출
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        }
        // 하나 이면
        else this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
