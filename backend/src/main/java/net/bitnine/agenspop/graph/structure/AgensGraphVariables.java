package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensGraphVariables implements Graph.Variables {

    private final AgensGraph graph;
    private final BaseGraphAPI baseGraph;

    private final Map<String, Object> variables = new ConcurrentHashMap<>();

    public AgensGraphVariables(final AgensGraph graph) {
        this.graph = graph;
        this.baseGraph = graph.getBaseGraph();
    }

    @Override
    public Set<String> keys() {
        return this.variables.keySet();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.variables.get(key));
    }

    @Override
    public void remove(final String key) {
        this.variables.remove(key);
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.variables.put(key, value);
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }

    /////////////////////////////////////////

    public static class AgensVariableFeatures implements Graph.Features.VariableFeatures {
        @Override public boolean supportsBooleanValues() { return true; }
        @Override public boolean supportsDoubleValues() { return true; }
        @Override public boolean supportsFloatValues() { return true; }
        @Override public boolean supportsIntegerValues() { return true; }
        @Override public boolean supportsLongValues() { return true; }
        @Override public boolean supportsMapValues() { return false; }
        @Override public boolean supportsMixedListValues() { return false; }
        @Override public boolean supportsByteValues() { return false; }
        @Override public boolean supportsBooleanArrayValues() { return true; }
        @Override public boolean supportsByteArrayValues() { return false; }
        @Override public boolean supportsDoubleArrayValues() { return true; }
        @Override public boolean supportsFloatArrayValues() { return true; }
        @Override public boolean supportsIntegerArrayValues() { return true; }
        @Override public boolean supportsLongArrayValues() { return true; }
        @Override public boolean supportsStringArrayValues() { return true; }
        @Override public boolean supportsSerializableValues() { return false; }
        @Override public boolean supportsStringValues() { return true; }
        @Override public boolean supportsUniformListValues() { return false; }
    }
}