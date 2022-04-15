package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.model.BaseElement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AgensElement implements Element, WrappedElement<BaseElement> {

    protected final AgensGraph graph;
    protected final BaseElement baseElement;

    protected AgensElement(final AgensGraph graph, final BaseElement baseElement) {
        this.graph = graph;
        this.baseElement = baseElement;
    }

    @Override
    public BaseElement getBaseElement() {
        return this.baseElement;
    }

    ///////////////////////////////////

    @Override
    public Object id() {
        this.graph.tx().readWrite();
        return this.baseElement.getId();
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.baseElement.getLabel();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.baseElement.keys()) {
            if (!Graph.Hidden.isHidden(key))
                keys.add(key);
        }
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }

}