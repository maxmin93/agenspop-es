package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensEdge extends AgensElement implements Edge, WrappedEdge<BaseEdge> {

    public AgensEdge(final AgensGraph graph, final BaseEdge edge) {
        super(graph, edge);
    }

    public AgensEdge(final AgensGraph graph, final Object id, final String label, final AgensVertex outVertex, final AgensVertex inVertex) {
        super(graph, graph.api.createEdge(graph.name(), id.toString(), label
                , outVertex.baseElement.getId(), inVertex.baseElement.getId()));
    }

    @Override
    public Vertex outVertex() {     // source v of edge
        Optional<? extends BaseVertex> v = this.graph.api.getVertexById(getBaseEdge().getSrc());
        if( v.isPresent() ){
            return (Vertex) new AgensVertex(this.graph, v.get());
        }
        return (Vertex) null;
    }

    @Override
    public Vertex inVertex() {      // target v of edge
        Optional<? extends BaseVertex> v = this.graph.api.getVertexById(getBaseEdge().getDst());
        if( v.isPresent() ){
            return (Vertex) new AgensVertex(this.graph, v.get());
        }
        return (Vertex) null;
    }

    @Override
    public BaseEdge getBaseEdge() {
        return (BaseEdge) this.baseElement;
    }

    public AgensEdge save(){
        this.graph.tx().readWrite();
        this.graph.api.saveEdge(getBaseEdge());
        return this;
    }

    ////////////////////////////////

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        Collection<String> keys = this.baseElement.keys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new AgensProperty<>(this, this.baseElement.getProperty(key)));
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();
        if( this.baseElement.hasProperty(key) )
            return new AgensProperty<>(this, this.baseElement.getProperty(key));
        else
            return Property.empty();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        BaseProperty propertyBase = this.graph.api.createProperty(key, value);

        this.graph.tx().readWrite();
        baseElement.setProperty(propertyBase);
        return new AgensProperty<V>(this, propertyBase);
    }

    ////////////////////////////////

    @Override
    public void remove() {
        if( this.baseElement.notexists() ) return;

        this.graph.tx().readWrite();
        // post processes of remove vertex : properties, graph, marking
        BaseEdge baseEdge = this.getBaseEdge();
        try {
            this.graph.api.dropEdge(baseEdge);
        }
        catch (RuntimeException e) {
            if (!AgensHelper.isNotFound(e)) throw e;
        }
    }

    @Override
    public String toString() {
        return "e[" + getBaseEdge().getId() + "]" + "[" + getBaseEdge().getSrc() + "->" + getBaseEdge().getDst() + "]";
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if ( this.baseElement.notexists() ) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex());  // source
            case IN:
                return IteratorUtils.of(this.inVertex());   // target
            default:
                return IteratorUtils.of(this.outVertex(), this.inVertex()); // BOTH
        }
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.getBaseEdge().getLabel();
    }
}