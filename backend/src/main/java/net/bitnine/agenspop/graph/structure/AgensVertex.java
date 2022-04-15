package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.*;
import java.util.stream.Stream;

public final class AgensVertex extends AgensElement implements Vertex, WrappedVertex<BaseVertex> {

    public AgensVertex(final AgensGraph graph, final BaseVertex vertex) {
        super(graph, vertex);
    }

    public AgensVertex(final AgensGraph graph, final Object id, final String label, final String name) {
        super(graph, graph.api.createVertex(graph.name(), id.toString(), label, name));
    }
    public AgensVertex(final AgensGraph graph, final Object id, final String label) {
        super(graph, graph.api.createVertex(graph.name(), id.toString(), label, id.toString()));
    }

    @Override
    public Graph graph(){ return this.graph; }

    @Override
    public BaseVertex getBaseVertex() {
        return (BaseVertex) this.baseElement;
    }

    public AgensVertex save(){
        this.graph.api.saveVertex(getBaseVertex());
        return this;
    }

    ////////////////////////////////////

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (inVertex == null) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.baseElement.notexists()) throw elementAlreadyRemoved(Vertex.class, this.id());
        return AgensHelper.addEdge(this.graph, this, (AgensVertex) inVertex, label, keyValues);
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        // 1) remove connected AgensEdges
        Stream<BaseEdge> edges = graph.api.findEdgesOfVertex(graph.name(), id().toString(), Direction.BOTH);
        edges.forEach(r->graph.api.dropEdge(r));
        // 2) remove AgensVertex
        this.graph.api.dropVertex((BaseVertex)baseElement);
        System.out.println("** remove vertex: "+toString());
    }

    // **NOTE: Cardinality.single 만 다룬다 ==> multi(set or list) 인 경우 Exception 처리
    //
    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality
            , final String key, final V value, final Object... keyValues) {

        if( this.baseElement.notexists() ) throw elementAlreadyRemoved(Vertex.class, this.id());
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);

        this.graph.tx().readWrite();
        final VertexProperty<V> vertexProperty = new AgensVertexProperty<>(this, key, value);
                // this.graph.trait.setVertexProperty(this, cardinality, key, value, keyValues);
        // rest keyValues
        if( keyValues.length > 0 ) ElementHelper.attachProperties(vertexProperty, keyValues);
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return baseElement.hasProperty(key) ?
                new AgensVertexProperty<>(this, baseElement.getProperty(key))
                : VertexProperty.empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return baseElement.keys().stream()
                .filter(k->ElementHelper.keyExists(k, propertyKeys))
                .map(k->(VertexProperty<V>) new AgensVertexProperty<V>(this, baseElement.getProperty(k)))
                .iterator();
    }

    // 정점의 이웃 정점들 (방향성, 연결간선의 라벨셋)
    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        this.graph.tx().readWrite();
        Stream<Vertex> stream = graph.api
                .findNeighborVertices(graph.name(), id().toString(), direction, edgeLabels)
                .map(r->(Vertex)new AgensVertex(graph, r));
        return stream.iterator();
    }

    // 정점의 연결 간선들 (방향, 연결간선의 라벨셋)
    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        this.graph.tx().readWrite();
        Stream<Edge> stream = graph.api
                .findEdgesOfVertex(graph.name(), id().toString(), direction, edgeLabels)
                .map(r->(Edge)new AgensEdge(graph, r));
        return stream.iterator();
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // **참고 https://github.com/rayokota/hgraphdb

    public Iterator<Edge> edges(final Direction direction, final String label, final String key, final Object value) {
        this.graph.tx().readWrite();
        Stream<Edge> stream = graph.api
                .findEdgesOfVertex(graph.name(), id().toString(), direction, label, key, value)
                .map(r->(Edge)new AgensEdge(graph, r));
        return stream.iterator();
    }

    //////////////////////////////////////////////////////////////////////////////////////

    @Override
    public String toString() { return "v[" + id() + "]"; }
}