package net.bitnine.agenspop.graph.process.traversal.step.sideEffect;

import net.bitnine.agenspop.graph.process.util.AgensElasticIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;

// 참고 org.apache.tinkerpop.gremlin.neo4j.process.traversal.step.sideEffect.CypherStartStep
//
public class ElasticStartStep extends StartStep<Map<String, Object>> {

    private final String query;

    public ElasticStartStep(final Traversal.Admin traversal, final String query, final AgensElasticIterator<?> elasticIterator) {
        super(traversal, elasticIterator);
        this.query = query;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.query);
    }
}
