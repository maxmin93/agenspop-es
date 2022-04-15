package net.bitnine.agenspop.basegraph;

import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public interface BaseGraphAPI {

    // enum Direction { BOTH, IN, OUT };        // use tinkerpop.core

    BaseTx tx();

    //////////////////////////////////////////////////
    //
    // common access services about ElasticElement
    //

    Stream<BaseVertex> vertices(String datasource);
    Stream<BaseEdge> edges(String datasource);

    Stream<BaseVertex> verticesByIds(String[] ids);
    Stream<BaseEdge> edgesByIds(String[] ids);

    boolean existsVertex(String id);
    boolean existsEdge(String id);

    Optional<BaseVertex> getVertexById(String id);
    Optional<BaseEdge> getEdgeById(String id);

    BaseVertex createVertex(String datasource, String id, String label, String name);
    BaseVertex createVertex(String datasource, String id, String label);
    BaseEdge createEdge(String datasource, String id, String label, String src, String dst);
    BaseProperty createProperty(String key, Object value);

    boolean saveVertex(BaseVertex vertex);
    boolean saveEdge(BaseEdge edge);

    void dropVertex(BaseVertex vertex);
    void dropVertex(String id);
    void dropEdge(BaseEdge edge);
    void dropEdge(String id);

    long countV(String datasource);
    long countE(String datasource);

    List<String> searchDatasources(String query);
    Map<String, Long> searchVertexDatasources(List<String> dsList);
    Map<String, Long> searchEdgeDatasources(List<String> dsList);

    Map<String, Long> listVertexDatasources();
    Map<String, Long> listEdgeDatasources();

    Map<String, Long> listVertexLabels(String datasource);
    Map<String, Long> listEdgeLabels(String datasource);

    Map<String, Long> listVertexLabelKeys(String datasource, String label);
    Map<String, Long> listEdgeLabelKeys(String datasource, String label);

    //////////////////////////////////////////////////
    //
    //  **NOTE: optimized find functions
    //      ==> http://tinkerpop.apache.org/docs/current/reference/#has-step
    //
    //    hasId(ids…​)              : findVertices(ids)
    //    hasLabel(labels…​)        : findVerticesWithLabels(ds, ...labels)
    //    has(key,value)           : findVerticesWithKV(ds, key, val)
    //    has(label, key, value)   : findVerticesWithLKV(ds, label, key, value)
    //    has(key)                 : findVerticesWithKey(ds, key)
    //    hasNot(key)              : findVerticesWithNotKey(ds, key)
    //    hasKey(keys…​)            : findVerticesWithKeys(ds, ...keys)
    //    hasValue(values…​)        : findVerticesWithValues(ds, ...values)
/*
    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    Collection<BaseVertex> findVertices(final String[] ids);
    Collection<BaseVertex> findVertices(String datasource, String label);
    Collection<BaseVertex> findVertices(String datasource, final String[] labels);
    Collection<BaseVertex> findVertices(String datasource, String key, String value);
    Collection<BaseVertex> findVertices(String datasource, String label, String key, String value);
    Collection<BaseVertex> findVertices(String datasource, String key, boolean hasNot);
    Collection<BaseVertex> findVerticesWithKeys(String datasource, final String[] keys);
    Collection<BaseVertex> findVerticesWithValue(String datasource, String value, boolean isPartial);
    Collection<BaseVertex> findVerticesWithValues(String datasource, final String[] values);
    Collection<BaseVertex> findVerticesWithKeyValues(String datasource, String label, Map<String,String> kvPairs);
    Collection<BaseVertex> findVertices(String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs);

    BaseVertex findOtherVertexOfEdge(String eid, String vid);
    Collection<BaseVertex> findNeighborVertices(String datasource, String vid, Direction direction, final String[] labels);


    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Collection<BaseEdge> findEdges(final String[] ids);
    Collection<BaseEdge> findEdges(String datasource, String label);
    Collection<BaseEdge> findEdges(String datasource, final String[] labels);
    Collection<BaseEdge> findEdges(String datasource, String key, String value);
    Collection<BaseEdge> findEdges(String datasource, String label, String key, String value);
    Collection<BaseEdge> findEdges(String datasource, String key, boolean hasNot);
    Collection<BaseEdge> findEdgesWithKeys(String datasource, final String[] keys);
    Collection<BaseEdge> findEdgesWithValue(String datasource, String value, boolean isPartial);
    Collection<BaseEdge> findEdgesWithValues(String datasource, final String[] values);
    Collection<BaseEdge> findEdgesWithKeyValues(String datasource, String label, Map<String,String> kvPairs);
    Collection<BaseEdge> findEdges(String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs);

    Collection<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction);
    Collection<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, final String[] labels);
    Collection<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, String label, String key, Object value);
*/
    //////////////////////////////////////////////////
    //
    // Stream APIs
    //
    //////////////////////////////////////////////////

    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    Stream<BaseVertex> findVertices(final String[] ids);
    Stream<BaseVertex> findVertices(String datasource, String label);
    Stream<BaseVertex> findVertices(String datasource, final String[] labels);
    Stream<BaseVertex> findVertices(String datasource, String key, String value);
    Stream<BaseVertex> findVertices(String datasource, String label, String key, String value);
    Stream<BaseVertex> findVertices(String datasource, String key, boolean hasNot);
    Stream<BaseVertex> findVerticesWithKeys(String datasource, final String[] keys);
    Stream<BaseVertex> findVerticesWithValue(String datasource, String value, boolean isPartial);
    Stream<BaseVertex> findVerticesWithValues(String datasource, final String[] values);
    Stream<BaseVertex> findVerticesWithKeyValues(String datasource, String label, Map<String,String> kvPairs);
    Stream<BaseVertex> findVertices(String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs);

    BaseVertex findOtherVertexOfEdge(String eid, String vid);
    Stream<BaseVertex> findNeighborVertices(String datasource, String vid, Direction direction, final String[] labels);

    Stream<BaseVertex> findVerticesWithDateRange(final String[] ids, String fromDate, String toDate);
    Stream<BaseVertex> findVerticesWithDateRange(String datasource, String fromDate, String toDate);
    Stream<BaseVertex> findVerticesWithDateRange(String datasource, String label, String fromDate, String toDate);


    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Stream<BaseEdge> findEdges(final String[] ids);
    Stream<BaseEdge> findEdges(String datasource, String label);
    Stream<BaseEdge> findEdges(String datasource, final String[] labels);
    Stream<BaseEdge> findEdges(String datasource, String key, String value);
    Stream<BaseEdge> findEdges(String datasource, String label, String key, String value);
    Stream<BaseEdge> findEdges(String datasource, String key, boolean hasNot);
    Stream<BaseEdge> findEdgesWithKeys(String datasource, final String[] keys);
    Stream<BaseEdge> findEdgesWithValue(String datasource, String value, boolean isPartial);
    Stream<BaseEdge> findEdgesWithValues(String datasource, final String[] values);
    Stream<BaseEdge> findEdgesWithKeyValues(String datasource, String label, Map<String,String> kvPairs);
    Stream<BaseEdge> findEdges(String datasource
            , String label, String[] labels
            , String key, String keyNot, String[] keys
            , String[] values, Map<String,String> kvPairs);

    Stream<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction);
    Stream<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, final String[] labels);
    Stream<BaseEdge> findEdgesOfVertex(String datasource, String vid, Direction direction, String label, String key, Object value);

    Stream<BaseEdge> findEdgesWithDateRange(final String[] ids, String fromDate, String toDate);
    Stream<BaseEdge> findEdgesWithDateRange(String datasource, String fromDate, String toDate);
    Stream<BaseEdge> findEdgesWithDateRange(String datasource, String label, String fromDate, String toDate);

}
