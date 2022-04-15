package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * Helps create a variety of different toy graphs for testing and learning purposes.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class AgensFactory {

    public static final AtomicInteger graphSeq = new AtomicInteger(0);
    public static final String GREMLIN_DEFAULT_GRAPH_NAME = "default";

    public static String defaultGraphName(){
        return GREMLIN_DEFAULT_GRAPH_NAME+graphSeq.incrementAndGet();
    }

    private static Configuration getMixIdManagerConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_GRAPH_NAME, defaultGraphName());
        // **NOTE: 필요 없음.
        //      AgensGraph 초기화시 IdManager 고정
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_VERTEX_ID_MANAGER, AgensIdManager.ANY.name());
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_EDGE_ID_MANAGER, AgensIdManager.ANY.name());
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.single);
        return conf;
    }

    private AgensFactory() {}

    /////////////////////////////////////

    public static AgensGraph createEmpty(BaseGraphAPI baseGraph, String gName) {
        final Configuration conf = getMixIdManagerConfiguration();
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_GRAPH_NAME, gName);
        return AgensGraph.open(baseGraph, conf);
    }

    /**
     * Create the "modern" graph which has the same structure as the "classic" graph from AgensPop 2.x but includes
     * 3.x features like vertex labels.
     */
    public static AgensGraph createModern(BaseGraphAPI baseGraph) {
        final Configuration conf = getMixIdManagerConfiguration();
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_GRAPH_NAME, "modern");
        final AgensGraph g = AgensGraph.open(baseGraph, conf);
        generateModern(g);
        return g;
    }

/*
g = TinkerGraph.open();

v1 = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29, "country", "USA");
v2 = g.addVertex(T.id, 2, "name", "vadas", "age", 27);
v3 = g.addVertex(T.id, 3, "name", "lop", "lang", "java");
v4 = g.addVertex(T.id, 4, "name", "josh", "age", 32);
v5 = g.addVertex(T.id, 5, "name", "ripple", "lang", "java");
v6 = g.addVertex(T.id, 6, "name", "peter", "age", 35);
e1 = v1.addEdge("knows", v2, T.id, 7, "weight", 0.5f);
e2 = v1.addEdge("knows", v4, T.id, 8, "weight", 1.0f);
e3 = v1.addEdge("created", v3, T.id, 9, "weight", 0.4f);
e4 = v4.addEdge("created", v5, T.id, 10, "weight", 1.0f);
e5 = v4.addEdge("created", v3, T.id, 11, "weight", 0.4f);
e6 = v6.addEdge("created", v3, T.id, 12, "weight", 0.2f);
 */
    public static void generateModern(final AgensGraph g) {
        final Vertex marko = g.addVertex(T.id, "modern_1", T.label, "person"
                , BaseElement.timestampTag, "2019-01-22T22:22:22"
                , "name", "marko", "age", 29, "country", "USA");
        final Vertex vadas = g.addVertex(T.id, "modern_2", T.label, "person"
                , BaseElement.timestampTag, "2019-03-22T22:22:22"
                , "name", "vadas", "age", 27, "country", "USA");
        final Vertex lop = g.addVertex(T.id, "modern_3", T.label, "software"
                , BaseElement.timestampTag, "2019-05-22T22:22:22"
                , "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.id, "modern_4", T.label, "person"
                , BaseElement.timestampTag, "2019-07-22T22:22:22"
                , "name", "josh", "age", 32, "country", "USA");
        final Vertex ripple = g.addVertex(T.id, "modern_5", T.label, "software"
                , BaseElement.timestampTag, "2019-09-22T22:22:22"
                , "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.id, "modern_6", T.label, "person"
                , BaseElement.timestampTag, "2019-11-22T22:22:22"
                , "name", "peter", "age", 35, "country", "USA");
        marko.addEdge("knows", vadas, T.id, "modern_7"
                , "weight", 0.5d, BaseElement.timestampTag, "2019-02-22T22:22:22");
        marko.addEdge("knows", josh, T.id, "modern_8"
                , "weight", 1.0d, BaseElement.timestampTag, "2019-04-22T22:22:22");
        marko.addEdge("created", lop, T.id, "modern_9"
                , "weight", 0.4d, BaseElement.timestampTag, "2019-06-22T22:22:22");
        josh.addEdge("created", ripple, T.id, "modern_10"
                , "weight", 1.0d, BaseElement.timestampTag, "2019-08-22T22:22:22");
        josh.addEdge("created", lop, T.id, "modern_11"
                , "weight", 0.4d, BaseElement.timestampTag, "2019-10-22T22:22:22");
        peter.addEdge("created", lop, T.id, "modern_12"
                , "weight", 0.2d, BaseElement.timestampTag, "2019-12-22T22:22:22");
        // without created value
        marko.addEdge("supports", ripple, T.id, "modern_13"
                , "weight", 0.4d);
        peter.addEdge("supports", ripple, T.id, "modern_14"
                , "weight", 0.2d);

        // addEdge Step
        // https://tinkerpop.apache.org/docs/current/reference/#addedge-step
        GraphTraversalSource t = g.traversal();
        Edge newEdge = t.V("modern_1").as("src").V("modern_2").as("dst")
                .addE("like").from("src").to("dst").property(T.id,"modern_15")
                .property("since",2017).next();
    }

    public static void traversalTestModern(final AgensGraph g){
        ///////////////////////////////////////////////////
        // gremlin test

        // remove test ==> vertex{1}, edge{7,8,9}
//        System.out.println("  - before remove V(marko): "+g.toString());
//        marko.remove();

        GraphTraversalSource t = g.traversal();
        List<Vertex> vertexList = t.V().next(100);
        System.out.println("  1) V.all ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));
        List<Edge> edgeList = t.E().next(100);
        System.out.println("  2) E.all ==> "+edgeList.stream().map(Edge::toString).collect(Collectors.joining(",")));
        vertexList = t.V("modern_5", "modern_4", "modern_3").next(100);
        System.out.println("  3) V(id..) ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));
        edgeList = t.V("modern_1").bothE().next(100);
        System.out.println("  4) V(id).bothE ==> "+edgeList.stream().map(Edge::toString).collect(Collectors.joining(",")));

        Vertex v1 = t.V("modern_1").next();
        vertexList = t.V(v1.id()).out().next(100);  // BUT, on groovy ==> g.V(v1).out()
        System.out.println("  5) V(id).out ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));

        List<Object> valueList = t.V().values("name").next(100);
        System.out.println("  6) V.values('name') ==> "+valueList.stream().map(v->String.valueOf(v)).collect(Collectors.joining(",")));
        vertexList = t.V().has("name","josh").next(100);
        System.out.println("  7) V.has(key,value) ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));

        edgeList = t.V().hasLabel("person").outE("knows").next(100);
        System.out.println("  8) V.hasLabel.outE ==> "+edgeList.stream().map(Edge::toString).collect(Collectors.joining(",")));
        vertexList = t.V().hasLabel("person").out().where(__.values("age").is(P.lt(30))).next(100);
        System.out.println("  9) V.where(age<30) ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));
    }
}