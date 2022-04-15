package net.bitnine.agenspop.graph.process.util;

import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensVertex;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

// 참고 org.apache.tinkerpop.gremlin.neo4j.process.util.Neo4jCypherIterator
//
public final class AgensElasticIterator<T> implements Iterator<Map<String, T>> {

    final Iterator<Map<String, T>> iterator;
    final AgensGraph graph;

    public AgensElasticIterator(final Iterator<Map<String, T>> iterator, final AgensGraph graph) {
        this.iterator = iterator;
        this.graph = graph;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Map<String, T> next() {
        return this.iterator.next().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    final T val = entry.getValue();
                    if (BaseVertex.class.isAssignableFrom(val.getClass())) {
                        return (T) new AgensVertex(graph, (BaseVertex)val);
                    } else if (BaseEdge.class.isAssignableFrom(val.getClass())) {
                        return (T) new AgensEdge(graph, (BaseEdge)val);
                    } else {
                        return val;
                    }
                }));
    }
}