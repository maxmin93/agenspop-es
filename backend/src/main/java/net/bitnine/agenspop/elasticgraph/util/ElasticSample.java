package net.bitnine.agenspop.elasticgraph.util;

import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import net.bitnine.agenspop.elasticgraph.model.ElasticProperty;
import net.bitnine.agenspop.elasticgraph.model.ElasticVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ElasticSample {

    public static List<ElasticVertex> modernVertices(){
        String datasource = "modern";

        ElasticVertex v1 = new ElasticVertex(datasource, "modern_1", "person");
        v1.properties( Arrays.asList(
                new ElasticProperty("name", "marko"),
                new ElasticProperty("age", 29),
                new ElasticProperty("country", "USA"),
                new ElasticProperty(BaseElement.timestampTag, "2019-01-21T21:21:21")
        ) );
        ElasticVertex v2 = new ElasticVertex(datasource, "modern_2", "person");
        v2.properties( Arrays.asList(
                new ElasticProperty("name", "vadas"),
                new ElasticProperty("age", 27),
                new ElasticProperty("country", "USA"),
                new ElasticProperty(BaseElement.timestampTag, "2019-03-23T23:23:23")
        ) );
        ElasticVertex v3 = new ElasticVertex(datasource, "modern_3", "software");
        v3.properties( Arrays.asList(
                new ElasticProperty("name", "lop"),
                new ElasticProperty("lang", "java"),
                new ElasticProperty(BaseElement.timestampTag, "2019-05-25T15:25:25")
        ) );
        ElasticVertex v4 = new ElasticVertex(datasource, "modern_4", "person");
        v4.properties( Arrays.asList(
                new ElasticProperty("name", "josh"),
                new ElasticProperty("age", 32),
                new ElasticProperty("country", "USA"),
                new ElasticProperty(BaseElement.timestampTag, "2019-07-27T21:21:21")
        ) );
        ElasticVertex v5 = new ElasticVertex(datasource, "modern_5", "software");
        v5.properties( Arrays.asList(
                new ElasticProperty("name", "ripple"),
                new ElasticProperty("lang", "java"),
                new ElasticProperty(BaseElement.timestampTag, "2019-09-29T21:21:21")
        ) );
        ElasticVertex v6 = new ElasticVertex(datasource, "modern_6", "person");
        v6.properties( Arrays.asList(
                new ElasticProperty("name", "peter"),
                new ElasticProperty("age", 35),
                new ElasticProperty("country", "USA"),
                new ElasticProperty(BaseElement.timestampTag, "2019-11-11T21:11:21")
        ) );

        return new ArrayList<>(Arrays.asList(v1, v2, v3, v4, v5, v6));
    }

    public static List<ElasticEdge> modernEdges() {
        String datasource = "modern";

        ElasticEdge e1 = new ElasticEdge(datasource, "modern_7", "knows", "modern_1", "modern_2");
        e1.properties( Arrays.asList( new ElasticProperty("weight", 0.5d)
                        , new ElasticProperty(BaseElement.timestampTag, "2019-02-11T19:11:21")));
        ElasticEdge e2 = new ElasticEdge(datasource, "modern_8", "knows", "modern_1", "modern_4");
        e2.properties( Arrays.asList( new ElasticProperty("weight", 1.0d)
                        , new ElasticProperty(BaseElement.timestampTag, "2019-04-11T19:11:21")));
        ElasticEdge e3 = new ElasticEdge(datasource, "modern_9", "created", "modern_1", "modern_3");
        e3.properties( Arrays.asList( new ElasticProperty("weight", 0.4d)
                        , new ElasticProperty(BaseElement.timestampTag, "2019-06-11T19:11:21")));
        ElasticEdge e4 = new ElasticEdge(datasource, "modern_10", "created", "modern_4", "modern_5");
        e4.properties( Arrays.asList( new ElasticProperty("weight", 1.5d)
                        , new ElasticProperty(BaseElement.timestampTag, "2019-08-11T19:11:21")));
        ElasticEdge e5 = new ElasticEdge(datasource, "modern_11", "created", "modern_4", "modern_3");
        e5.properties( Arrays.asList( new ElasticProperty("weight", 0.4d)
                        , new ElasticProperty(BaseElement.timestampTag, "2019-10-11T19:11:21")));
        ElasticEdge e6 = new ElasticEdge(datasource, "modern_12", "created", "modern_6", "modern_3");
        e6.properties( Arrays.asList( new ElasticProperty("weight", 0.2d)
                        , new ElasticProperty(BaseElement.timestampTag, "2019-12-11T19:11:21")));

        return new ArrayList<>(Arrays.asList(e1, e2, e3, e4, e5, e6));
    }
}
