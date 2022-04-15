package net.bitnine.agenspop.graph.process.traversal.step.map;


import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensCountGlobalStep<S extends Element> extends AbstractStep<S, Long> {

    private final Class<S> elementClass;
    private boolean done = false;

    public AgensCountGlobalStep(final Traversal.Admin traversal, final Class<S> elementClass) {
        super(traversal);
        this.elementClass = elementClass;
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        if (!this.done) {
            this.done = true;
            final AgensGraph graph = (AgensGraph) this.getTraversal().getGraph().get();
            return this.getTraversal().getTraverserGenerator().generate(
                        Vertex.class.isAssignableFrom(this.elementClass) ?
                            (long) graph.getBaseGraph().countV(graph.name())
                            : (long) graph.getBaseGraph().countE(graph.name())
                    , (Step) this, 1L);
        } else
            throw FastNoSuchElementException.instance();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.elementClass.getSimpleName().toLowerCase());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.elementClass.hashCode();
    }

    @Override
    public void reset() {
        this.done = false;
    }
}